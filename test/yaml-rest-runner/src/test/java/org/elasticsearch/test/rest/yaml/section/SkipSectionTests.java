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
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SkipSectionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testSkipVersionMultiRange() {
        SkipSection section = new SkipSection(
            List.of(SkipCriteria.fromVersionRange("6.0.0 - 6.1.0, 7.1.0 - 7.5.0")),
            Collections.emptyList(),
            "foobar"
        );

        var outOfRangeMockContext = mock(ClientYamlTestExecutionContext.class);
        when(outOfRangeMockContext.nodesVersions()).thenReturn(Set.of(Version.CURRENT.toString()))
            .thenReturn(Set.of("6.2.0"))
            .thenReturn(Set.of("7.0.0"))
            .thenReturn(Set.of("7.6.0"));

        assertFalse(section.skip(outOfRangeMockContext));
        assertFalse(section.skip(outOfRangeMockContext));
        assertFalse(section.skip(outOfRangeMockContext));
        assertFalse(section.skip(outOfRangeMockContext));

        var inRangeMockContext = mock(ClientYamlTestExecutionContext.class);
        when(inRangeMockContext.nodesVersions()).thenReturn(Set.of("6.0.0"))
            .thenReturn(Set.of("6.1.0"))
            .thenReturn(Set.of("7.1.0"))
            .thenReturn(Set.of("7.5.0"));

        assertTrue(section.skip(inRangeMockContext));
        assertTrue(section.skip(inRangeMockContext));
        assertTrue(section.skip(inRangeMockContext));
        assertTrue(section.skip(inRangeMockContext));
    }

    public void testSkipVersionMultiOpenRange() {
        var section = new SkipSection(
            List.of(SkipCriteria.fromVersionRange("-  7.1.0, 7.2.0 - 7.5.0, 8.0.0 -")),
            Collections.emptyList(),
            "foobar"
        );

        var outOfRangeMockContext = mock(ClientYamlTestExecutionContext.class);
        when(outOfRangeMockContext.nodesVersions()).thenReturn(Set.of("7.1.1")).thenReturn(Set.of("7.6.0"));

        assertFalse(section.skip(outOfRangeMockContext));
        assertFalse(section.skip(outOfRangeMockContext));

        var inRangeMockContext = mock(ClientYamlTestExecutionContext.class);
        when(inRangeMockContext.nodesVersions()).thenReturn(Set.of("7.0.0"))
            .thenReturn(Set.of("7.3.0"))
            .thenReturn(Set.of("8.0.0"))
            .thenReturn(Set.of(Version.CURRENT.toString()));

        assertTrue(section.skip(inRangeMockContext));
        assertTrue(section.skip(inRangeMockContext));
        assertTrue(section.skip(inRangeMockContext));
        assertTrue(section.skip(inRangeMockContext));
    }

    public void testSkipVersion() {
        SkipSection section = new SkipSection(List.of(SkipCriteria.fromVersionRange("6.0.0 - 6.1.0")), Collections.emptyList(), "foobar");

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.nodesVersions()).thenReturn(Set.of(Version.CURRENT.toString()))
            .thenReturn(Set.of("6.0.0"))
            .thenReturn(Set.of("6.0.0", "6.1.0"))
            .thenReturn(Set.of("6.0.0", "5.2.0"));

        assertFalse(section.skip(mockContext));
        assertTrue(section.skip(mockContext));
        assertTrue(section.skip(mockContext));
        assertFalse(section.skip(mockContext));
    }

    public void testSkipVersionWithTestFeatures() {
        SkipSection section = new SkipSection(
            List.of(SkipCriteria.fromVersionRange("6.0.0 - 6.1.0")),
            Collections.singletonList("warnings"),
            "foobar"
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.nodesVersions()).thenReturn(Set.of(Version.CURRENT.toString())).thenReturn(Set.of("6.0.0"));

        assertFalse(section.skip(mockContext));
        assertTrue(section.skip(mockContext));
    }

    public void testSkipTestFeatures() {
        var section = new SkipSection.SkipSectionBuilder().skipIfYamlRunnerFeatureAbsent("boom").build();
        assertTrue(section.skip(mock(ClientYamlTestExecutionContext.class)));
    }

    public void testSkipTestFeaturesOverridesAnySkipCriteria() {
        var section = new SkipSection.SkipSectionBuilder().skipOs("test-os").skipIfYamlRunnerFeatureAbsent("boom").build();

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.os()).thenReturn("test-os");

        // Skip even if OS is matching
        assertTrue(section.skip(mockContext));
    }

    public void testSkipOs() {
        SkipSection section = new SkipSection.SkipSectionBuilder().skipOs("windows95").skipOs("debian-5").build();

        var mockContext = mock(ClientYamlTestExecutionContext.class);

        when(mockContext.os()).thenReturn("debian-5");
        assertTrue(section.skip(mockContext));

        when(mockContext.os()).thenReturn("windows95");
        assertTrue(section.skip(mockContext));

        when(mockContext.os()).thenReturn("ms-dos");
        assertFalse(section.skip(mockContext));
    }

    public void testSkipOsWithTestFeatures() {
        SkipSection section = new SkipSection.SkipSectionBuilder().skipIfYamlRunnerFeatureAbsent("warnings")
            .skipOs("windows95")
            .skipOs("debian-5")
            .build();

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.os()).thenReturn("debian-5");
        assertTrue(section.skip(mockContext));

        when(mockContext.os()).thenReturn("windows95");
        assertTrue(section.skip(mockContext));

        when(mockContext.os()).thenReturn("ms-dos");
        assertFalse(section.skip(mockContext));
    }

    public void testMessage() {
        SkipSection section = new SkipSection(
            List.of(SkipCriteria.fromVersionRange("6.0.0 - 6.1.0")),
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
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures.size(), equalTo(0));
        assertThat(skipSectionBuilder.reason, equalTo("Delete ignores the parent param"));
    }

    public void testParseSkipSectionFeatureNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "features:     regex");

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex"));
        assertThat(skipSectionBuilder.reason, nullValue());
        assertThat(skipSectionBuilder.xpackRequired, is(SkipSection.SkipSectionBuilder.XPackRequired.NOT_SPECIFIED));
    }

    public void testParseXPackFeature() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, "features:     xpack");

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, empty());
        assertThat(skipSectionBuilder.reason, nullValue());
        assertThat(skipSectionBuilder.xpackRequired, is(SkipSection.SkipSectionBuilder.XPackRequired.YES));
    }

    public void testParseNoXPackFeature() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, "features:     no_xpack");

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, empty());
        assertThat(skipSectionBuilder.reason, nullValue());
        assertThat(skipSectionBuilder.xpackRequired, is(SkipSection.SkipSectionBuilder.XPackRequired.NO));
    }

    public void testParseBothXPackFeatures() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, "features:     [xpack, no_xpack]");

        var e = expectThrows(ParsingException.class, () -> SkipSection.parseInternal(parser));
        assertThat(e.getMessage(), containsString("either `xpack` or `no_xpack` can be present, not both"));
    }

    public void testParseSkipSectionFeaturesNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "features:     [regex1,regex2,regex3]");

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex1", "regex2", "regex3"));
        assertThat(skipSectionBuilder.reason, nullValue());
    }

    public void testParseSkipSectionBothFeatureAndVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            version:     " - 0.90.2"
            features:     regex
            reason:      Delete ignores the parent param""");

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder.version, not(emptyOrNullString()));
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex"));
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
        assertThat(
            e.getMessage(),
            is("at least one criteria (version, cluster features, runner features, os) is mandatory within a skip section")
        );
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
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, hasSize(2));
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
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, hasSize(1));
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

    public void testParseSkipSectionRequireClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            cluster_features_absent:          needed-feature
            reason:      test skipped when cluster lacks needed-feature
            """);

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredClusterFeatures, contains("needed-feature"));
        assertThat(skipSectionBuilder.reason, is("test skipped when cluster lacks needed-feature"));
    }

    public void testParseSkipSectionSkipClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            cluster_features_present:          undesired-feature
            reason:      test skipped when undesired-feature is present
            """);

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.forbiddenClusterFeatures, contains("undesired-feature"));
        assertThat(skipSectionBuilder.reason, is("test skipped when undesired-feature is present"));
    }

    public void testParseSkipSectionRequireAndSkipClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            cluster_features_absent:        needed-feature
            cluster_features_present:          undesired-feature
            reason:      test need needed-feature to run, but not when undesired-feature is present
            """);

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.forbiddenClusterFeatures, contains("undesired-feature"));
        assertThat(skipSectionBuilder.requiredClusterFeatures, contains("needed-feature"));
        assertThat(skipSectionBuilder.reason, is("test need needed-feature to run, but not when undesired-feature is present"));
    }

    public void testParseSkipSectionRequireAndSkipMultipleClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            cluster_features_absent:        [needed-feature-1, needed-feature-2]
            cluster_features_present:          [undesired-feature-1, undesired-feature-2]
            reason:      test needs some to run, but not when others are present
            """);

        var skipSectionBuilder = SkipSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.version, emptyOrNullString());
        assertThat(skipSectionBuilder.forbiddenClusterFeatures, containsInAnyOrder("undesired-feature-1", "undesired-feature-2"));
        assertThat(skipSectionBuilder.requiredClusterFeatures, containsInAnyOrder("needed-feature-1", "needed-feature-2"));
        assertThat(skipSectionBuilder.reason, is("test needs some to run, but not when others are present"));
    }

    public void testParseSkipSectionSameRequireAndSkipClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            cluster_features_absent:        some-feature
            cluster_features_present:          some-feature
            reason:      test needs some-feature to run, but not when some-feature is present
            """);

        var e = expectThrows(ParsingException.class, () -> SkipSection.parseInternal(parser));
        assertThat(e.getMessage(), is("skip on a cluster feature can be when it is either present or missing, not both"));
    }

    public void testSkipClusterFeaturesAllRequiredMatch() {
        SkipSection section = new SkipSection(
            List.of(SkipCriteria.fromClusterFeatures(Set.of("required-feature-1", "required-feature-2"), Set.of())),
            Collections.emptyList(),
            "foobar"
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1")).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2")).thenReturn(true);

        assertFalse(section.skip(mockContext));
    }

    public void testSkipClusterFeaturesSomeRequiredMatch() {
        SkipSection section = new SkipSection(
            List.of(SkipCriteria.fromClusterFeatures(Set.of("required-feature-1", "required-feature-2"), Set.of())),
            Collections.emptyList(),
            "foobar"
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1")).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2")).thenReturn(false);

        assertTrue(section.skip(mockContext));
    }

    public void testSkipClusterFeaturesSomeToSkipMatch() {
        SkipSection section = new SkipSection(
            List.of(SkipCriteria.fromClusterFeatures(Set.of(), Set.of("undesired-feature-1", "undesired-feature-2"))),
            Collections.emptyList(),
            "foobar"
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("undesired-feature-1")).thenReturn(true);

        assertTrue(section.skip(mockContext));
    }

    public void testSkipClusterFeaturesNoneToSkipMatch() {
        SkipSection section = new SkipSection(
            List.of(SkipCriteria.fromClusterFeatures(Set.of(), Set.of("undesired-feature-1", "undesired-feature-2"))),
            Collections.emptyList(),
            "foobar"
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        assertFalse(section.skip(mockContext));
    }

    public void testSkipClusterFeaturesAllRequiredSomeToSkipMatch() {
        SkipSection section = new SkipSection(
            List.of(
                SkipCriteria.fromClusterFeatures(
                    Set.of("required-feature-1", "required-feature-2"),
                    Set.of("undesired-feature-1", "undesired-feature-2")
                )
            ),
            Collections.emptyList(),
            "foobar"
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1")).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2")).thenReturn(true);
        when(mockContext.clusterHasFeature("undesired-feature-1")).thenReturn(true);

        assertTrue(section.skip(mockContext));
    }

    public void testSkipClusterFeaturesAllRequiredNoneToSkipMatch() {
        SkipSection section = new SkipSection(
            List.of(
                SkipCriteria.fromClusterFeatures(
                    Set.of("required-feature-1", "required-feature-2"),
                    Set.of("undesired-feature-1", "undesired-feature-2")
                )
            ),
            Collections.emptyList(),
            "foobar"
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1")).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2")).thenReturn(true);

        assertFalse(section.skip(mockContext));
    }
}
