"""Regression tests for Lucene snapshot index-version updates.

Run with: python3 -m unittest discover -s .buildkite/scripts/lucene-snapshot -v
"""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from update_index_versions import INDEX_VERSIONS, update_index_versions


SOURCE = """public class IndexVersions {
    public static final IndexVersion PREVIOUS = def(9_129_0_00, Version.LUCENE_10_5_2);
    public static final IndexVersion UPGRADE_TO_LUCENE_10_6_0 = def(9_300_00_0, Version.LUCENE_10_6_0);
    /* Instructions for adding new index versions. */
}
"""
EXPECTED = SOURCE.replace(
    "    /* Instructions",
    "    public static final IndexVersion UPGRADE_TO_LUCENE_10_6_1 = def(9_301_0_00, Version.LUCENE_10_6_1);\n"
    "    /* Instructions",
)


class UpdateIndexVersionsTests(unittest.TestCase):
    def test_cli_generates_expected_line_in_index_versions_file(self):
        helper = Path(__file__).with_name("update_index_versions.py").resolve()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / INDEX_VERSIONS
            path.parent.mkdir(parents=True)
            path.write_text(SOURCE)
            subprocess.run([sys.executable, str(helper), "10.6.1"], cwd=directory, check=True, capture_output=True)
            self.assertEqual(EXPECTED, path.read_text())

    def test_same_version_leaves_file_unchanged(self):
        self.assertEqual(SOURCE, update_index_versions(SOURCE, "10.6.0"))
        self.assertEqual(EXPECTED, update_index_versions(EXPECTED, "10.6.1"))

    def test_patch_minor_and_major_lucene_upgrades(self):
        for version in ("10.6.1", "10.7.0", "11.0.0"):
            with self.subTest(version=version):
                expected = EXPECTED.replace("10_6_1", version.replace(".", "_"))
                self.assertEqual(expected, update_index_versions(SOURCE, version))

    def test_next_id_resets_patch_part(self):
        source = SOURCE.replace("9_300_00_0", "9_300_0_01")
        expected = EXPECTED.replace("9_300_00_0", "9_300_0_01")
        self.assertEqual(expected, update_index_versions(source, "10.6.1"))

    def test_multiline_definition(self):
        source = SOURCE.replace("def(9_300_00_0, Version.LUCENE_10_6_0)", "def(\n        9_300_00_0,\n        Version.LUCENE_10_6_0\n    )")
        expected = EXPECTED.replace("def(9_300_00_0, Version.LUCENE_10_6_0)", "def(\n        9_300_00_0,\n        Version.LUCENE_10_6_0\n    )")
        self.assertEqual(expected, update_index_versions(source, "10.6.1"))

    def test_rejects_downgrade(self):
        with self.assertRaisesRegex(ValueError, "downgrade"):
            update_index_versions(SOURCE, "10.5.2")

    def test_rejects_invalid_version(self):
        with self.assertRaisesRegex(ValueError, "numeric Lucene version"):
            update_index_versions(SOURCE, "10.6.1-snapshot-abcdef")

    def test_rejects_missing_or_unrecognized_definitions(self):
        with self.assertRaisesRegex(ValueError, "Could not find"):
            update_index_versions("class IndexVersions {}", "10.6.1")
        source = SOURCE.replace("Version.LUCENE_10_6_0", "Version.LATEST")
        with self.assertRaisesRegex(ValueError, "Unrecognized"):
            update_index_versions(source, "10.6.1")


if __name__ == "__main__":
    unittest.main()
