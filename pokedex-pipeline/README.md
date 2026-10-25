# Pokémon Pokédex Data Pipeline

Builds a normalized JSON + CSV Pokédex from the structured CSV tables in PokeAPI's repository. It does not scrape Pokémon Database, Bulbapedia, or Serebii. Those are reserved for targeted manual cross-checks.

## What it exports

- Species identity, generation, growth rate and species metadata
- Individual Pokémon records / forms, stats, typing and ability slots
- Evolution edges and evolution condition metadata
- Moves, abilities and version-specific learnsets
- Machine compatibility where represented by upstream tables
- Pokédex entry metadata **without copying entry prose**
- Source manifest and coverage report

## Requirements

Python 3.11+ and network access.

```bash
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Run

```bash
python -m src.build_pokedex --download
```

To rebuild from the raw snapshot without downloading again:

```bash
python -m src.build_pokedex
```

Raw files are stored in `data/raw/pokeapi/`. Normalized tables and exports are written under `data/exports/`.

## Important completeness rules

- The build reports source coverage; it does not claim completeness if required tables are missing.
- `modern_profile` is selected dynamically from configured preferred version groups that exist in the snapshot. The report shows the chosen version group and any preferred versions absent from the snapshot.
- Pokédex entry text is omitted by default. `pokedex_entries.csv` stores source metadata only, with `entry_text` empty and `redistribution_status=not_included_rights_unverified`.
- Learnsets retain version-group IDs and learning methods. Do not treat all-generation compatibility as compatibility with a single modern game.
- `pokemon_forms.csv` and `pokemon.csv` are distinct upstream concepts. The output keeps individual Pokémon/form records linked to species.
- Mega, regional, cosmetic and battle-only forms are represented when present in the pinned upstream snapshot. A missing form in the source snapshot cannot be invented by the pipeline.
- The upstream `master` URL is mutable. For a release, pin a commit SHA and replace the raw CSV base with that commit URL in `config/sources.json`.

## Outputs

- `data/exports/json/pokedex.json`
- `data/exports/json/manifest.json`
- normalized CSV tables in `data/exports/csv/`
- `manifests/build_report.json`

## Limitations

The source is PokeAPI's `master` branch, which changes over time. Each build writes SHA-256 hashes of the raw files to `manifests/build_report.json`, and that report is the authority on what was actually collected and validated. `config/sources.json` (mentioned above) is not implemented yet; for now, pinning means editing `BASE_URL` in `src/build_pokedex.py`.
