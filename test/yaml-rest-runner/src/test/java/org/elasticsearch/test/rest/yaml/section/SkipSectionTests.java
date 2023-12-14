/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SkipSectionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testSkipVersionMultiRange() {
        SkipSection section = new SkipSection(
            List.of(new VersionSkipCriteria("6.0.0 - 6.1.0, 7.1.0 - 7.5.0")),
            Collections.emptyList(),
            "foobar"
        );

        assertFalse(section.skip(versionOnlyContext(Version.CURRENT)));
        assertFalse(section.skip(versionOnlyContext(Version.fromString("6.2.0"))));
        assertFalse(section.skip(versionOnlyContext(Version.fromString("7.0.0"))));
        assertFalse(section.skip(versionOnlyContext(Version.fromString("7.6.0"))));

        assertTrue(section.skip(versionOnlyContext(Version.fromString("6.0.0"))));
        assertTrue(section.skip(versionOnlyContext(Version.fromString("6.1.0"))));
        assertTrue(section.skip(versionOnlyContext(Version.fromString("7.1.0"))));
        assertTrue(section.skip(versionOnlyContext(Version.fromString("7.5.0"))));

        section = new SkipSection(List.of(new VersionSkipCriteria("-  7.1.0, 7.2.0 - 7.5.0, 8.0.0 -")), Collections.emptyList(), "foobar");
        assertTrue(section.skip(versionOnlyContext(Version.fromString("7.0.0"))));
        assertTrue(section.skip(versionOnlyContext(Version.fromString("7.3.0"))));
        assertTrue(section.skip(versionOnlyContext(Version.fromString("8.0.0"))));
    }

    public void testSkipVersion() {
        SkipSection section = new SkipSection(List.of(new VersionSkipCriteria("6.0.0 - 6.1.0")), Collections.emptyList(), "foobar");
        assertFalse(section.skip(versionOnlyContext(Version.CURRENT)));
        assertTrue(section.skip(versionOnlyContext(Version.fromString("6.0.0"))));
    }

    public void testSkipVersionWithTestFeatures() {
        SkipSection section = new SkipSection(
            List.of(new VersionSkipCriteria("6.0.0 - 6.1.0")),
            Collections.singletonList("warnings"),
            "foobar"
        );
        assertFalse(section.skip(versionOnlyContext(Version.CURRENT)));
        assertTrue(section.skip(versionOnlyContext(Version.fromString("6.0.0"))));
    }

    public void testSkipTestFeatures() {
        var section = new SkipSection(randomBoolean() ? List.of() : List.of(context -> false), Collections.singletonList("boom"), "foobar");
        assertTrue(section.skip(versionOnlyContext(Version.CURRENT)));
    }

    public void testSkipOs() {
        var osList = List.of("windows95", "debian-5");
        SkipSection section = new SkipSection(
            List.of(new OsSkipCriteria(osList)),
            randomBoolean() ? Collections.emptyList() : Collections.singletonList("warnings"),
            "foobar"
        );
        assertTrue(section.skip(osOnlyContext("windows95")));
        assertTrue(section.skip(osOnlyContext("debian-5")));
        assertFalse(section.skip(osOnlyContext("ms-dos")));
    }

    public void testMessage() {
        SkipSection section = new SkipSection(
            List.of(new VersionSkipCriteria("6.0.0 - 6.1.0")),
            Collections.singletonList("warnings"),
            "foobar"
        );
        assertEquals("[FOOBAR] skipped, reason: [foobar] unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
        section = new SkipSection(List.of(), Collections.singletonList("warnings"), "foobar");
        assertEquals("[FOOBAR] skipped, reason: [foobar] unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
        section = new SkipSection(List.of(), Collections.singletonList("warnings"), null);
        assertEquals("[FOOBAR] skipped, unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
    }

    public void testParseSkipSectionVersionNoFeature() throws Exception {
        Version version = VersionUtils.randomVersion(random());
        parser = createParser(YamlXContent.yamlXContent, Strings.format("""
            version:     " - %s"
            reason:      Delete ignores the parent param""", version));

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, not(emptyOrNullString()));
        assertThat(skipSectionBuilder.testFeatures.size(), equalTo(0));
        assertThat(skipSectionBuilder.reason, equalTo("Delete ignores the parent param"));
    }

    public void testParseSkipSectionFeatureNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "features:     regex");

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.testFeatures, hasSize(1));
        assertThat(skipSectionBuilder.testFeatures, contains("regex"));
        assertThat(skipSectionBuilder.reason, nullValue());
    }

    public void testParseSkipSectionFeaturesNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "features:     [regex1,regex2,regex3]");

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.testFeatures, hasSize(3));
        assertThat(skipSectionBuilder.testFeatures, contains("regex1", "regex2", "regex3"));
        assertThat(skipSectionBuilder.reason, nullValue());
    }

    public void testParseSkipSectionBothFeatureAndVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            version:     " - 0.90.2"
            features:     regex
            reason:      Delete ignores the parent param""");

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder.version, not(emptyOrNullString()));
        assertThat(skipSectionBuilder.testFeatures, contains("regex"));
        assertThat(skipSectionBuilder.reason, equalTo("Delete ignores the parent param"));
    }

    public void testParseSkipSectionNoReason() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "version:     \" - 0.90.2\"\n");

        Exception e = expectThrows(ParsingException.class, () -> SkipSection.parseInternal(parser));
        assertThat(e.getMessage(), is("reason is mandatory within skip version section"));
    }

    public void testParseSkipSectionNoVersionNorFeature() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "reason:      Delete ignores the parent param\n");

        Exception e = expectThrows(ParsingException.class, () -> SkipSection.parseInternal(parser));
        assertThat(e.getMessage(), is("at least one criteria (version, test features, os) is mandatory within a skip section"));
    }

    public void testParseSkipSectionOsNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            features:    ["skip_os", "some_feature"]
            os:          debian-9
            reason:      memory accounting broken, see gh#xyz
            """);

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.testFeatures, hasSize(2));
        assertThat(skipSectionBuilder.operatingSystems, contains("debian-9"));
        assertThat(skipSectionBuilder.reason, is("memory accounting broken, see gh#xyz"));
    }

    public void testParseSkipSectionOsListNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            features:    skip_os
            os:          [debian-9,windows-95,ms-dos]
            reason:      see gh#xyz
            """);

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.testFeatures, hasSize(1));
        assertThat(skipSectionBuilder.operatingSystems, containsInAnyOrder("debian-9", "windows-95", "ms-dos"));
        assertThat(skipSectionBuilder.reason, is("see gh#xyz"));
    }

    public void testParseSkipSectionOsNoFeatureNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            os:          debian-9
            reason:      memory accounting broken, see gh#xyz
            """);

        Exception e = expectThrows(ParsingException.class, () -> SkipSection.parseInternal(parser));
        assertThat(e.getMessage(), is("if os is specified, feature skip_os must be set"));
    }
}
