"""Keep snapshot builds' current index version aligned with their Lucene dependency."""

import argparse
import re
from pathlib import Path


INDEX_VERSIONS = Path("server/src/main/java/org/elasticsearch/index/IndexVersions.java")


def update_index_versions(text: str, lucene_version: str) -> str:
    """Append a new mapping without changing historical index-version definitions."""
    if re.fullmatch(r"\d+\.\d+\.\d+", lucene_version) is None:
        raise ValueError(f"Expected a numeric Lucene version, got {lucene_version!r}")
    version = tuple(map(int, lucene_version.split(".")))
    entries = list(re.finditer(
        r"    public static final IndexVersion \w+ = def\(\s*([\d_]+),\s*(.*?)\s*\);",
        text,
        re.DOTALL,
    ))
    if not entries:
        raise ValueError("Could not find index-version definitions")
    latest = max(entries, key=lambda entry: int(entry[1].replace("_", "")))
    current_match = re.fullmatch(r"Version\.LUCENE_(\d+)_(\d+)_(\d+)", latest[2])
    if current_match is None:
        raise ValueError(f"Unrecognized current Lucene version: {latest[2]}")
    current = tuple(map(int, current_match.groups()))
    if version < current:
        raise ValueError(f"Refusing to downgrade the current index version from Lucene {current} to {version}")
    if version == current:
        return text

    # Advance the server part and reset the subsidiary and patch parts (M_NNN_S_PP).
    major, server = divmod(int(latest[1].replace("_", "")) // 1000, 1000)
    if server == 999:
        raise ValueError("Cannot allocate another index version within this Elasticsearch major")
    version_snake = "_".join(map(str, version))
    name = f"UPGRADE_TO_LUCENE_{version_snake}"
    if re.search(rf"\b{re.escape(name)}\b", text):
        raise ValueError(f"Index-version constant {name} already exists")
    definition = (
        f"\n    public static final IndexVersion {name} = "
        f"def({major}_{server + 1:03d}_0_00, Version.LUCENE_{version_snake});"
    )
    return text[:latest.end()] + definition + text[latest.end():]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lucene_version", help="Numeric Lucene version, e.g. 10.6.1")
    args = parser.parse_args()
    text = INDEX_VERSIONS.read_text()
    try:
        updated = update_index_versions(text, args.lucene_version)
    except ValueError as error:
        parser.error(str(error))
    if updated != text:
        INDEX_VERSIONS.write_text(updated)
        print(f"Added index version for Lucene {args.lucene_version}")


if __name__ == "__main__":
    main()
