package org.elasticsearch.gradle.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.gradle.test.DistroTestPlugin.deriveId;
import static org.elasticsearch.gradle.test.DistroTestPlugin.parseOsRelease;
import static org.hamcrest.CoreMatchers.equalTo;

public class DistroTestPluginTests extends GradleIntegrationTestCase {

    public void testParseOsReleaseOnOracle() {
        final List<String> lines = List.of(
            "NAME=\"Oracle Linux Server\"",
            "VERSION=\"6.10\"",
            "ID=\"ol\"",
            "VERSION_ID=\"6.10\"",
            "PRETTY_NAME=\"Oracle Linux Server 6.10\"",
            "ANSI_COLOR=\"0;31\"",
            "CPE_NAME=\"cpe:/o:oracle:linux:6:10:server\"",
            "HOME_URL" + "=\"https://linux.oracle.com/\"",
            "BUG_REPORT_URL=\"https://bugzilla.oracle.com/\"",
            "",
            "ORACLE_BUGZILLA_PRODUCT" + "=\"Oracle Linux 6\"",
            "ORACLE_BUGZILLA_PRODUCT_VERSION=6.10",
            "ORACLE_SUPPORT_PRODUCT=\"Oracle Linux\"",
            "ORACLE_SUPPORT_PRODUCT_VERSION=6.10"
        );

        final Map<String, String> results = parseOsRelease(lines);

        final Map<String, String> expected = new HashMap<>();
        expected.put("ANSI_COLOR", "0;31");
        expected.put("BUG_REPORT_URL", "https://bugzilla.oracle.com/");
        expected.put("CPE_NAME", "cpe:/o:oracle:linux:6:10:server");
        expected.put("HOME_URL" + "", "https://linux.oracle.com/");
        expected.put("ID", "ol");
        expected.put("NAME", "Oracle Linux Server");
        expected.put("ORACLE_BUGZILLA_PRODUCT" + "", "Oracle Linux 6");
        expected.put("ORACLE_BUGZILLA_PRODUCT_VERSION", "6.10");
        expected.put("ORACLE_SUPPORT_PRODUCT", "Oracle Linux");
        expected.put("ORACLE_SUPPORT_PRODUCT_VERSION", "6.10");
        expected.put("PRETTY_NAME", "Oracle Linux Server 6.10");
        expected.put("VERSION", "6.10");
        expected.put("VERSION_ID", "6.10");

        assertThat(expected, equalTo(results));
    }

    /**
     * Trailing whitespace should be removed
     */
    public void testRemoveTrailingWhitespace() {
        final List<String> lines = List.of("NAME=\"Oracle Linux Server\"   ");

        final Map<String, String> results = parseOsRelease(lines);

        final Map<String, String> expected = Map.of("NAME", "Oracle Linux Server");

        assertThat(expected, equalTo(results));
    }

    /**
     * Comments should be removed
     */
    public void testRemoveComments() {
        final List<String> lines = List.of("# A comment", "NAME=\"Oracle Linux Server\"");

        final Map<String, String> results = parseOsRelease(lines);

        final Map<String, String> expected = Map.of("NAME", "Oracle Linux Server");

        assertThat(expected, equalTo(results));
    }

    public void testDeriveIdOnOracle() {
        final Map<String, String> osRelease = new HashMap<>();
        osRelease.put("ID", "ol");
        osRelease.put("VERSION_ID", "6.10");

        assertThat("ol-6.10", equalTo(deriveId(osRelease)));
    }
}
