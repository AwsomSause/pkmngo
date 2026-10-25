from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "pokeapi"
NORM_DIR = ROOT / "data" / "normalized"
EXPORT_DIR = ROOT / "data" / "exports"
JSON_DIR = EXPORT_DIR / "json"
CSV_DIR = EXPORT_DIR / "csv"
MANIFEST_DIR = ROOT / "manifests"

BASE_URL = "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/"
TABLES = [
    "pokemon_species.csv", "pokemon.csv", "pokemon_forms.csv",
    "pokemon_stats.csv", "pokemon_abilities.csv", "pokemon_types.csv",
    "pokemon_species_names.csv", "pokemon_species_flavor_text.csv",
    "pokemon_form_names.csv", "pokemon_form_generations.csv",
    "pokemon_moves.csv", "moves.csv", "move_names.csv",
    "move_flavor_text.csv", "pokemon_evolution.csv",
    "abilities.csv", "ability_names.csv", "ability_flavor_text.csv",
    "types.csv", "type_names.csv", "stats.csv", "growth_rates.csv",
    "experience.csv", "pokemon_dex_numbers.csv", "pokemon_egg_groups.csv",
    "egg_groups.csv", "pokemon_game_indices.csv", "versions.csv",
    "version_groups.csv", "machines.csv", "pokemon_move_methods.csv",
    "languages.csv", "pokemon_colors.csv", "pokemon_shapes.csv",
    "pokemon_habitats.csv", "genders.csv", "pokemon_species_prose.csv"
]
REQUIRED = {
    "pokemon_species.csv", "pokemon.csv", "pokemon_stats.csv",
    "pokemon_abilities.csv", "pokemon_types.csv", "pokemon_moves.csv",
    "moves.csv", "pokemon_evolution.csv", "abilities.csv", "types.csv",
    "stats.csv", "version_groups.csv", "versions.csv"
}
PREFERRED_VERSION_GROUPS = ["legends-z-a", "scarlet-violet"]
USER_AGENT = "pokemon-pokedex-data-pipeline/0.1 (personal research; respectful bulk CSV fetch)"

def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def safe_int(value: Any):
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def boolish(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes"}

def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def download_tables(refresh: bool = False) -> dict[str, Any]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    result = {"downloaded": [], "cached": [], "missing": [], "errors": []}
    for name in TABLES:
        dest = RAW_DIR / name
        if dest.exists() and dest.stat().st_size and not refresh:
            result["cached"].append(name)
            continue
        url = BASE_URL + name
        try:
            response = session.get(url, timeout=60)
            if response.status_code == 404:
                result["missing"].append(name)
                continue
            response.raise_for_status()
            if not response.content.strip():
                result["missing"].append(name)
                continue
            dest.write_bytes(response.content)
            result["downloaded"].append(name)
            print(f"Downloaded {name} ({len(response.content):,} bytes)")
        except Exception as exc:
            result["errors"].append({"file": name, "error": str(exc)})
            print(f"WARNING: {name}: {exc}", file=sys.stderr)
    return result

def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        keys = set()
        for row in rows:
            keys.update(row.keys())
        fieldnames = sorted(keys)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v for k, v in row.items()})

def write_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False) + "\n", encoding="utf-8")

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def build(refresh: bool = False, download_result: dict[str, Any] | None = None):
    if refresh:
        download_result = download_tables(refresh=True)
    elif download_result is None:
        download_result = {"downloaded": [], "cached": [], "missing": [], "errors": []}
    tables = {p.name: load_csv(p) for p in RAW_DIR.glob("*.csv")}
    missing_required = sorted(name for name in REQUIRED if not tables.get(name))
    if missing_required:
        if not refresh:
            print("Required source tables missing. Retry with --download (or --refresh).", file=sys.stderr)
        report = {
            "build_timestamp": now_iso(), "status": "blocked_missing_source_tables",
            "missing_required_tables": missing_required, "download": download_result,
            "source_table_counts": {k: len(v) for k, v in tables.items()}
        }
        MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
        write_json(MANIFEST_DIR / "build_report.json", report)
        raise SystemExit("Build stopped: missing required source tables: " + ", ".join(missing_required))

    def rows(name): return tables.get(name, [])
    def index(name, key):
        out = {}
        for r in rows(name):
            val = r.get(key)
            if val not in (None, ""):
                out[str(val)] = r
        return out

    species_by_id = index("pokemon_species.csv", "id")
    pokemon_by_id = index("pokemon.csv", "id")
    moves_by_id = index("moves.csv", "id")
    abilities_by_id = index("abilities.csv", "id")
    types_by_id = index("types.csv", "id")
    stats_by_id = index("stats.csv", "id")
    growth_by_id = index("growth_rates.csv", "id")
    version_groups_by_id = index("version_groups.csv", "id")
    versions_by_id = index("versions.csv", "id")
    methods_by_id = index("pokemon_move_methods.csv", "id")
    form_by_pokemon = defaultdict(list)
    for f in rows("pokemon_forms.csv"):
        form_by_pokemon[str(f.get("pokemon_id"))].append(f)

    # Names, localized names, and localized descriptions. English is language id 9 in PokeAPI.
    species_name_rows = rows("pokemon_species_names.csv")
    species_names = {}
    for r in species_name_rows:
        if str(r.get("local_language_id")) == "9":
            species_names[str(r.get("pokemon_species_id"))] = r.get("name")
    move_names = {}
    for r in rows("move_names.csv"):
        if str(r.get("local_language_id")) == "9":
            move_names[str(r.get("move_id"))] = r.get("name")
    ability_names = {}
    for r in rows("ability_names.csv"):
        if str(r.get("local_language_id")) == "9":
            ability_names[str(r.get("ability_id"))] = r.get("name")
    type_names = {}
    for r in rows("type_names.csv"):
        if str(r.get("local_language_id")) == "9":
            type_names[str(r.get("type_id"))] = r.get("name")
    form_names = {}
    for r in rows("pokemon_form_names.csv"):
        if str(r.get("local_language_id")) == "9":
            form_names[str(r.get("pokemon_form_id"))] = r.get("pokemon_name") or r.get("form_name")

    # Pivot stats and link to individual Pokémon IDs.
    stats_pivot = defaultdict(dict)
    ev_yields = defaultdict(dict)
    stat_identifiers = {str(v["id"]): v.get("identifier") for v in rows("stats.csv")}
    for r in rows("pokemon_stats.csv"):
        pid, sid = str(r.get("pokemon_id")), str(r.get("stat_id"))
        ident = stat_identifiers.get(sid, "")
        field = {"hp":"hp", "attack":"attack", "defense":"defense",
                 "special-attack":"special_attack", "special-defense":"special_defense",
                 "speed":"speed"}.get(ident)
        if field:
            stats_pivot[pid][field] = safe_int(r.get("base_stat"))
            ev_yields[pid][field] = safe_int(r.get("effort"))

    types_for_pokemon = defaultdict(list)
    for r in rows("pokemon_types.csv"):
        tid = str(r.get("type_id"))
        types_for_pokemon[str(r.get("pokemon_id"))].append({
            "slot": safe_int(r.get("slot")),
            "type_id": safe_int(tid),
            "type": types_by_id.get(tid, {}).get("identifier"),
            "name": type_names.get(tid) or types_by_id.get(tid, {}).get("identifier")
        })
    for vals in types_for_pokemon.values():
        vals.sort(key=lambda x: x["slot"] or 0)

    abilities_for_pokemon = defaultdict(list)
    for r in rows("pokemon_abilities.csv"):
        aid = str(r.get("ability_id"))
        abilities_for_pokemon[str(r.get("pokemon_id"))].append({
            "ability_id": safe_int(aid),
            "name": abilities_by_id.get(aid, {}).get("identifier"),
            "display_name": ability_names.get(aid) or abilities_by_id.get(aid, {}).get("identifier"),
            "slot": safe_int(r.get("slot")),
            "is_hidden": boolish(r.get("is_hidden"))
        })
    for vals in abilities_for_pokemon.values():
        vals.sort(key=lambda x: x["slot"] or 0)

    # Evolution condition rows are keyed by evolved species. Parent species are supplied by species.evolves_from_species_id.
    evolution_conditions = defaultdict(list)
    for r in rows("pokemon_evolution.csv"):
        evolved = str(r.get("evolved_species_id"))
        condition = {k: (safe_int(v) if k not in {"time_of_day", "relative_physical_stats", "needs_overworld_rain", "turn_upside_down"} else (boolish(v) if k in {"needs_overworld_rain", "turn_upside_down"} else v))
                     for k, v in r.items() if k not in {"id", "evolved_species_id"} and v not in (None, "")}
        evolution_conditions[evolved].append(condition)

    species_records = []
    evolution_records = []
    for sid, s in sorted(species_by_id.items(), key=lambda kv: int(kv[0])):
        growth_id = str(s.get("growth_rate_id", ""))
        rec = {
            "species_id": safe_int(sid),
            "national_dex_number": safe_int(s.get("id")),
            "name": s.get("identifier"),
            "display_name": species_names.get(sid) or s.get("identifier"),
            "generation_id": safe_int(s.get("generation_id")),
            "evolves_from_species_id": safe_int(s.get("evolves_from_species_id")),
            "evolution_chain_id": safe_int(s.get("evolution_chain_id")),
            "growth_rate_id": safe_int(growth_id),
            "growth_rate": growth_by_id.get(growth_id, {}).get("identifier"),
            "gender_rate": safe_int(s.get("gender_rate")),
            "capture_rate": safe_int(s.get("capture_rate")),
            "base_happiness": safe_int(s.get("base_happiness")),
            "is_baby": boolish(s.get("is_baby")),
            "hatch_counter": safe_int(s.get("hatch_counter")),
            "has_gender_differences": boolish(s.get("has_gender_differences")),
            "is_legendary": boolish(s.get("is_legendary")),
            "is_mythical": boolish(s.get("is_mythical")),
            "color_id": safe_int(s.get("color_id")),
            "shape_id": safe_int(s.get("shape_id")),
            "habitat_id": safe_int(s.get("habitat_id")),
            "egg_groups": [],
            "source_url": f"https://pokeapi.co/api/v2/pokemon-species/{sid}/"
        }
        species_records.append(rec)
        parent = safe_int(s.get("evolves_from_species_id"))
        if parent:
            evolution_records.append({
                "from_species_id": parent,
                "to_species_id": safe_int(sid),
                "trigger_conditions": evolution_conditions.get(sid, []),
                "evolution_chain_id": safe_int(s.get("evolution_chain_id")),
                "source_url": f"https://pokeapi.co/api/v2/pokemon-species/{sid}/"
            })

    egg_groups_by_species = defaultdict(list)
    egg_group_names = index("egg_groups.csv", "id")
    for r in rows("pokemon_egg_groups.csv"):
        egg_groups_by_species[str(r.get("species_id"))].append({
            "egg_group_id": safe_int(r.get("egg_group_id")),
            "name": egg_group_names.get(str(r.get("egg_group_id")), {}).get("identifier")
        })
    for rec in species_records:
        rec["egg_groups"] = egg_groups_by_species.get(str(rec["species_id"]), [])

    pokemon_records = []
    form_records = []
    for pid, p in sorted(pokemon_by_id.items(), key=lambda kv: int(kv[0])):
        sid = str(p.get("species_id"))
        form_candidates = form_by_pokemon.get(pid, [])
        form = next((f for f in form_candidates if boolish(f.get("is_default"))), None)
        if not form and form_candidates:
            form = form_candidates[0]
        form_id = safe_int(form.get("id")) if form else None
        base_stats = stats_pivot.get(pid, {})
        stat_total = sum(v for v in base_stats.values() if isinstance(v, int)) if len(base_stats) == 6 else None
        rec = {
            "pokemon_id": safe_int(pid),
            "species_id": safe_int(sid),
            "name": p.get("identifier"),
            "display_name": species_names.get(sid) or species_by_id.get(sid, {}).get("identifier"),
            "height_dm": safe_int(p.get("height")),
            "weight_hg": safe_int(p.get("weight")),
            "base_experience": safe_int(p.get("base_experience")),
            "is_default": boolish(p.get("is_default")),
            "form_id": form_id,
            "form_identifier": form.get("form_identifier") if form else None,
            "form_name": form_names.get(str(form_id)) if form_id else None,
            "is_battle_only": boolish(form.get("is_battle_only")) if form else False,
            "is_mega": boolish(form.get("is_mega")) if form else False,
            "introduced_in_version_group_id": safe_int(form.get("introduced_in_version_group_id")) if form else None,
            "types": types_for_pokemon.get(pid, []),
            "abilities": abilities_for_pokemon.get(pid, []),
            "stats": base_stats,
            "base_stat_total": stat_total,
            "ev_yield": ev_yields.get(pid, {}),
            "source_url": f"https://pokeapi.co/api/v2/pokemon/{pid}/"
        }
        pokemon_records.append(rec)
        for f in form_candidates:
            form_records.append({
                "form_id": safe_int(f.get("id")),
                "form_identifier": f.get("form_identifier"),
                "pokemon_id": safe_int(f.get("pokemon_id")),
                "pokemon_identifier": p.get("identifier"),
                "species_id": safe_int(sid),
                "introduced_in_version_group_id": safe_int(f.get("introduced_in_version_group_id")),
                "is_default": boolish(f.get("is_default")),
                "is_battle_only": boolish(f.get("is_battle_only")),
                "is_mega": boolish(f.get("is_mega")),
                "form_order": safe_int(f.get("form_order")),
                "order": safe_int(f.get("order")),
                "display_name": form_names.get(str(f.get("id"))),
                "source_url": f"https://pokeapi.co/api/v2/pokemon-form/{f.get('id')}/"
            })

    move_records = []
    for mid, m in sorted(moves_by_id.items(), key=lambda kv: int(kv[0])):
        move_records.append({
            "move_id": safe_int(mid), "name": m.get("identifier"),
            "display_name": move_names.get(mid) or m.get("identifier"),
            "generation_id": safe_int(m.get("generation_id")),
            "type_id": safe_int(m.get("type_id")),
            "type": types_by_id.get(str(m.get("type_id")), {}).get("identifier"),
            "damage_class_id": safe_int(m.get("damage_class_id")),
            "power": safe_int(m.get("power")), "pp": safe_int(m.get("pp")),
            "accuracy": safe_int(m.get("accuracy")), "priority": safe_int(m.get("priority")),
            "target_id": safe_int(m.get("target_id")),
            "effect_id": safe_int(m.get("effect_id")),
            "effect_chance": safe_int(m.get("effect_chance")),
            "contest_type_id": safe_int(m.get("contest_type_id")),
            "source_url": f"https://pokeapi.co/api/v2/move/{mid}/"
        })

    ability_records = []
    for aid, a in sorted(abilities_by_id.items(), key=lambda kv: int(kv[0])):
        ability_records.append({
            "ability_id": safe_int(aid), "name": a.get("identifier"),
            "display_name": ability_names.get(aid) or a.get("identifier"),
            "generation_id": safe_int(a.get("generation_id")),
            "is_main_series": boolish(a.get("is_main_series")),
            "source_url": f"https://pokeapi.co/api/v2/ability/{aid}/"
        })

    learnsets = []
    for r in rows("pokemon_moves.csv"):
        pid = safe_int(r.get("pokemon_id"))
        mid = safe_int(r.get("move_id"))
        vg_id = str(r.get("version_group_id"))
        method_id = str(r.get("pokemon_move_method_id"))
        learnsets.append({
            "pokemon_id": pid,
            "species_id": safe_int(pokemon_by_id.get(str(pid), {}).get("species_id")),
            "move_id": mid,
            "version_group_id": safe_int(vg_id),
            "version_group": version_groups_by_id.get(vg_id, {}).get("identifier"),
            "learn_method_id": safe_int(method_id),
            "learn_method": methods_by_id.get(method_id, {}).get("identifier"),
            "level": safe_int(r.get("level")),
            "order": safe_int(r.get("order"))
        })
    learnsets.sort(key=lambda r: (r["pokemon_id"] or 0, r["version_group_id"] or 0, r["learn_method_id"] or 0, r["level"] or 0, r["move_id"] or 0))

    machine_records = []
    for r in rows("machines.csv"):
        vg_id = str(r.get("version_group_id"))
        machine_records.append({
            "machine_number": safe_int(r.get("machine_number")),
            "version_group_id": safe_int(vg_id),
            "version_group": version_groups_by_id.get(vg_id, {}).get("identifier"),
            "item_id": safe_int(r.get("item_id")),
            "move_id": safe_int(r.get("move_id")),
            "move_name": moves_by_id.get(str(r.get("move_id")), {}).get("identifier")
        })

    # Metadata-only Pokédex entries: no flavor text is exported.
    entry_meta = []
    for r in rows("pokemon_species_flavor_text.csv"):
        sid = safe_int(r.get("species_id"))
        vid = safe_int(r.get("version_id"))
        lang = safe_int(r.get("language_id"))
        entry_meta.append({
            "species_id": sid, "version_id": vid,
            "version_name": versions_by_id.get(str(vid), {}).get("identifier"),
            "language_id": lang,
            "entry_text": "",
            "redistribution_status": "not_included_rights_unverified",
            "source_url": f"https://pokeapi.co/api/v2/pokemon-species/{sid}/"
        })
    # De-duplicate metadata rows, since the source may repeat a species/version/language.
    seen_meta, entry_meta_unique = set(), []
    for r in entry_meta:
        key = (r["species_id"], r["version_id"], r["language_id"])
        if key not in seen_meta:
            seen_meta.add(key)
            entry_meta_unique.append(r)

    version_group_records = [{
        "version_group_id": safe_int(vg_id),
        "name": v.get("identifier"),
        "generation_id": safe_int(v.get("generation_id")),
        "versions": [x.get("identifier") for x in rows("versions.csv") if str(x.get("version_group_id")) == vg_id]
    } for vg_id, v in sorted(version_groups_by_id.items(), key=lambda kv: int(kv[0]))]

    # Pick modern profile from configured candidates present in the current snapshot.
    present_names = {r["name"] for r in version_group_records}
    selected_profile = next((n for n in PREFERRED_VERSION_GROUPS if n in present_names), None)
    missing_preferred = [n for n in PREFERRED_VERSION_GROUPS if n not in present_names]

    # Normalize tables as standalone CSVs.
    output_tables = {
        "species.csv": species_records,
        "pokemon.csv": pokemon_records,
        "forms.csv": form_records,
        "evolutions.csv": evolution_records,
        "moves.csv": move_records,
        "abilities.csv": ability_records,
        "learnsets.csv": learnsets,
        "machines.csv": machine_records,
        "pokedex_entries.csv": entry_meta_unique,
        "version_groups.csv": version_group_records
    }
    for filename, data in output_tables.items():
        write_csv(NORM_DIR / filename, data)
        write_csv(CSV_DIR / filename, data)

    # Master JSON is linked/normalized; no official flavor text prose.
    master = {
        "schema_version": "1.0.0",
        "dataset_version": "0.1.0",
        "build_timestamp": now_iso(),
        "source": {
            "name": "PokeAPI",
            "repository": "https://github.com/PokeAPI/pokeapi",
            "raw_csv_base": BASE_URL,
            "license_note": "Review and preserve the applicable upstream license notices for the pinned source snapshot."
        },
        "modern_profile": {
            "selected_version_group": selected_profile,
            "preferred_candidates": PREFERRED_VERSION_GROUPS,
            "preferred_candidates_missing_from_snapshot": missing_preferred
        },
        "species": species_records,
        "pokemon": pokemon_records,
        "forms": form_records,
        "evolutions": evolution_records,
        "moves": move_records,
        "abilities": ability_records,
        "learnsets": learnsets,
        "machines": machine_records,
        "pokedex_entries": entry_meta_unique,
        "version_groups": version_group_records
    }
    write_json(JSON_DIR / "pokedex.json", master)

    raw_manifest = []
    for p in sorted(RAW_DIR.glob("*.csv")):
        raw_manifest.append({"file": p.name, "bytes": p.stat().st_size, "sha256": sha256(p)})
    required_missing = sorted(name for name in REQUIRED if not tables.get(name))
    counts = {k: len(v) for k, v in output_tables.items()}
    # Validation findings are explicit; a populated source snapshot is not proof of complete official coverage.
    findings = []
    species_ids = {r["species_id"] for r in species_records}
    pokemon_ids = {r["pokemon_id"] for r in pokemon_records}
    move_ids = {r["move_id"] for r in move_records}
    for p in pokemon_records:
        if p["species_id"] not in species_ids:
            findings.append({"severity":"error","code":"orphan_pokemon_species","pokemon_id":p["pokemon_id"]})
        if len(p["stats"]) != 6:
            findings.append({"severity":"error","code":"incomplete_base_stats","pokemon_id":p["pokemon_id"],"stat_count":len(p["stats"])})
        if not p["types"]:
            findings.append({"severity":"error","code":"missing_types","pokemon_id":p["pokemon_id"]})
    for e in evolution_records:
        if e["from_species_id"] not in species_ids or e["to_species_id"] not in species_ids:
            findings.append({"severity":"error","code":"orphan_evolution","edge":e})
    missing_move_refs = sum(1 for l in learnsets if l["move_id"] not in move_ids)
    if missing_move_refs:
        findings.append({"severity":"error","code":"missing_learnset_move_refs","count":missing_move_refs})
    duplicate_species = len(species_records) - len({r["species_id"] for r in species_records})
    if duplicate_species:
        findings.append({"severity":"error","code":"duplicate_species_ids","count":duplicate_species})
    source_hashes = {x["file"]: x["sha256"] for x in raw_manifest}
    report = {
        "build_timestamp": now_iso(),
        "status": "validation_passed_with_warnings" if not any(f["severity"] == "error" for f in findings) else "validation_failed",
        "source": {"name":"PokeAPI","repository":"https://github.com/PokeAPI/pokeapi","raw_csv_base":BASE_URL},
        "download": download_result,
        "source_table_counts": {k: len(v) for k, v in tables.items()},
        "output_counts": counts,
        "unique_counts": {
            "species": len(species_ids), "pokemon_records": len(pokemon_ids),
            "moves": len(move_ids), "learnset_rows": len(learnsets),
            "forms": len(form_records), "evolution_edges": len(evolution_records)
        },
        "modern_profile": {
            "selected_version_group": selected_profile,
            "preferred_candidates_missing_from_snapshot": missing_preferred
        },
        "validation": {
            "passed": not any(f["severity"] == "error" for f in findings),
            "error_count": sum(1 for f in findings if f["severity"] == "error"),
            "finding_count": len(findings),
            "findings": findings[:1000]
        },
        "raw_files": raw_manifest,
        "notes": [
            "Coverage is measured against this source snapshot, not independently certified official completeness.",
            "Pokédex entry text is intentionally excluded; metadata and source references only are exported.",
            "The source repository URL is mutable unless the build is pinned to a commit SHA.",
            "This build does not independently verify the correctness of all upstream records."
        ]
    }
    write_json(MANIFEST_DIR / "build_report.json", report)
    write_json(JSON_DIR / "manifest.json", report)
    print(json.dumps({
        "status": report["status"], "output_counts": counts,
        "modern_profile": report["modern_profile"],
        "validation": report["validation"]["passed"],
        "report": str(MANIFEST_DIR / "build_report.json")
    }, indent=2))
    if not report["validation"]["passed"]:
        raise SystemExit(2)

def main():
    parser = argparse.ArgumentParser(description="Build normalized Pokémon Pokédex JSON and CSV exports from PokeAPI CSV data.")
    parser.add_argument("--download", action="store_true", help="Download missing source CSV tables before building.")
    parser.add_argument("--refresh", action="store_true", help="Re-download source CSV tables before building.")
    args = parser.parse_args()
    if args.download or args.refresh:
        # download_tables is called inside build when refresh=True; for --download we download missing first.
        if args.download and not args.refresh:
            result = download_tables(refresh=False)
            build_result = build(refresh=False, download_result=result)
        else:
            build_result = build(refresh=True)
    else:
        build_result = build(refresh=False)

if __name__ == "__main__":
    main()
