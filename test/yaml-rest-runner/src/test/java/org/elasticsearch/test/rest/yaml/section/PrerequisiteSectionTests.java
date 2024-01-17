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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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

public class PrerequisiteSectionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testSkipVersionMultiRange() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnVersionRange("6.0.0 - 6.1.0, 7.1.0 - 7.5.0")),
            "foobar",
            emptyList(),
            "foobar",
            emptyList()
        );

        var outOfRangeMockContext = mock(ClientYamlTestExecutionContext.class);
        when(outOfRangeMockContext.nodesVersions()).thenReturn(Set.of(Version.CURRENT.toString()))
            .thenReturn(Set.of("6.2.0"))
            .thenReturn(Set.of("7.0.0"))
            .thenReturn(Set.of("7.6.0"));

        assertFalse(section.skipCriteriaMet(outOfRangeMockContext));
        assertFalse(section.skipCriteriaMet(outOfRangeMockContext));
        assertFalse(section.skipCriteriaMet(outOfRangeMockContext));
        assertFalse(section.skipCriteriaMet(outOfRangeMockContext));

        var inRangeMockContext = mock(ClientYamlTestExecutionContext.class);
        when(inRangeMockContext.nodesVersions()).thenReturn(Set.of("6.0.0"))
            .thenReturn(Set.of("6.1.0"))
            .thenReturn(Set.of("7.1.0"))
            .thenReturn(Set.of("7.5.0"));

        assertTrue(section.skipCriteriaMet(inRangeMockContext));
        assertTrue(section.skipCriteriaMet(inRangeMockContext));
        assertTrue(section.skipCriteriaMet(inRangeMockContext));
        assertTrue(section.skipCriteriaMet(inRangeMockContext));
    }

    public void testSkipVersionMultiOpenRange() {
        var section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnVersionRange("-  7.1.0, 7.2.0 - 7.5.0, 8.0.0 -")),
            "foobar",
            emptyList(),
            "foobar",
            emptyList()
        );

        var outOfRangeMockContext = mock(ClientYamlTestExecutionContext.class);
        when(outOfRangeMockContext.nodesVersions()).thenReturn(Set.of("7.1.1")).thenReturn(Set.of("7.6.0"));

        assertFalse(section.skipCriteriaMet(outOfRangeMockContext));
        assertFalse(section.skipCriteriaMet(outOfRangeMockContext));

        var inRangeMockContext = mock(ClientYamlTestExecutionContext.class);
        when(inRangeMockContext.nodesVersions()).thenReturn(Set.of("7.0.0"))
            .thenReturn(Set.of("7.3.0"))
            .thenReturn(Set.of("8.0.0"))
            .thenReturn(Set.of(Version.CURRENT.toString()));

        assertTrue(section.skipCriteriaMet(inRangeMockContext));
        assertTrue(section.skipCriteriaMet(inRangeMockContext));
        assertTrue(section.skipCriteriaMet(inRangeMockContext));
        assertTrue(section.skipCriteriaMet(inRangeMockContext));
    }

    public void testSkipVersion() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnVersionRange("6.0.0 - 6.1.0")),
            "foobar",
            emptyList(),
            "foobar",
            emptyList()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.nodesVersions()).thenReturn(Set.of(Version.CURRENT.toString()))
            .thenReturn(Set.of("6.0.0"))
            .thenReturn(Set.of("6.0.0", "6.1.0"))
            .thenReturn(Set.of("6.0.0", "5.2.0"));

        assertFalse(section.skipCriteriaMet(mockContext));
        assertTrue(section.skipCriteriaMet(mockContext));
        assertTrue(section.skipCriteriaMet(mockContext));
        assertFalse(section.skipCriteriaMet(mockContext));
    }

    public void testSkipVersionWithTestFeatures() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnVersionRange("6.0.0 - 6.1.0")),
            "foobar",
            emptyList(),
            "foobar",
            singletonList("warnings")
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.nodesVersions()).thenReturn(Set.of(Version.CURRENT.toString())).thenReturn(Set.of("6.0.0"));

        assertFalse(section.skipCriteriaMet(mockContext));
        assertTrue(section.skipCriteriaMet(mockContext));
    }

    public void testSkipTestFeatures() {
        var section = new PrerequisiteSection.PrerequisiteSectionBuilder().requireYamlRunnerFeature("boom").build();
        assertFalse(section.requiresCriteriaMet(mock(ClientYamlTestExecutionContext.class)));
    }

    public void testSkipTestFeaturesOverridesAnySkipCriteria() {
        var section = new PrerequisiteSection.PrerequisiteSectionBuilder().skipIfOs("test-os").requireYamlRunnerFeature("boom").build();

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.os()).thenReturn("test-os");

        // Skip even if OS is matching
        assertFalse(section.skipCriteriaMet(mockContext));
        assertFalse(section.requiresCriteriaMet(mockContext));
    }

    public void testSkipOs() {
        PrerequisiteSection section = new PrerequisiteSection.PrerequisiteSectionBuilder().skipIfOs("windows95")
            .skipIfOs("debian-5")
            .build();

        var mockContext = mock(ClientYamlTestExecutionContext.class);

        when(mockContext.os()).thenReturn("debian-5");
        assertTrue(section.skipCriteriaMet(mockContext));

        when(mockContext.os()).thenReturn("windows95");
        assertTrue(section.skipCriteriaMet(mockContext));

        when(mockContext.os()).thenReturn("ms-dos");
        assertFalse(section.skipCriteriaMet(mockContext));

        assertTrue(section.requiresCriteriaMet(mockContext));
    }

    public void testSkipOsWithTestFeatures() {
        PrerequisiteSection section = new PrerequisiteSection.PrerequisiteSectionBuilder().requireYamlRunnerFeature("warnings")
            .skipIfOs("windows95")
            .skipIfOs("debian-5")
            .build();

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.os()).thenReturn("debian-5");
        assertTrue(section.skipCriteriaMet(mockContext));

        when(mockContext.os()).thenReturn("windows95");
        assertTrue(section.skipCriteriaMet(mockContext));

        when(mockContext.os()).thenReturn("ms-dos");
        assertFalse(section.skipCriteriaMet(mockContext));
    }

    public void testBuildMessage() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnVersionRange("6.0.0 - 6.1.0")),
            "unsupported",
            emptyList(),
            "required",
            singletonList("warnings")
        );
        assertEquals("[FOOBAR] skipped, reason: [unsupported] unsupported features [warnings]", section.buildMessage("FOOBAR", true));
        assertEquals("[FOOBAR] skipped, reason: [required] unsupported features [warnings]", section.buildMessage("FOOBAR", false));
        section = new PrerequisiteSection(emptyList(), "unsupported", emptyList(), "required", emptyList());
        assertEquals("[FOOBAR] skipped, reason: [unsupported]", section.buildMessage("FOOBAR", true));
        assertEquals("[FOOBAR] skipped, reason: [required]", section.buildMessage("FOOBAR", false));
        section = new PrerequisiteSection(emptyList(), null, emptyList(), null, singletonList("warnings"));
        assertEquals("[FOOBAR] skipped, unsupported features [warnings]", section.buildMessage("FOOBAR", true));
        assertEquals("[FOOBAR] skipped, unsupported features [warnings]", section.buildMessage("FOOBAR", false));
    }

    public void testParseNoPrerequisites() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, """
            do:
               something
            """);
        var skipSectionBuilder = PrerequisiteSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());

        var skipSection = skipSectionBuilder.build();
        assertThat(skipSection.isEmpty(), equalTo(true));

        // Ensure the input (bogus execute section) was not consumed
        var next = ParserUtils.parseField(parser);
        assertThat(next, notNullValue());
        assertThat(parser.nextToken(), nullValue());
    }

    public void testParseSkipSectionVersionNoFeature() throws Exception {
        Version version = VersionUtils.randomVersion(random());
        parser = createParser(YamlXContent.yamlXContent, Strings.format("""
            version:     " - %s"
            reason:      Delete ignores the parent param""", version));

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, not(emptyOrNullString()));
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures.size(), equalTo(0));
        assertThat(skipSectionBuilder.skipReason, equalTo("Delete ignores the parent param"));
    }

    public void testParseSkipSectionFeatureNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "features:     regex");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex"));
        assertThat(skipSectionBuilder.skipReason, nullValue());
        assertThat(skipSectionBuilder.xpackRequired, is(PrerequisiteSection.PrerequisiteSectionBuilder.XPackRequired.NOT_SPECIFIED));
    }

    public void testParseXPackFeature() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, "features:     xpack");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, empty());
        assertThat(skipSectionBuilder.skipReason, nullValue());
        assertThat(skipSectionBuilder.xpackRequired, is(PrerequisiteSection.PrerequisiteSectionBuilder.XPackRequired.YES));
    }

    public void testParseNoXPackFeature() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, "features:     no_xpack");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, empty());
        assertThat(skipSectionBuilder.skipReason, nullValue());
        assertThat(skipSectionBuilder.xpackRequired, is(PrerequisiteSection.PrerequisiteSectionBuilder.XPackRequired.NO));
    }

    public void testParseBothXPackFeatures() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
               features:     [xpack, no_xpack]
            """);

        var e = expectThrows(ParsingException.class, () -> PrerequisiteSection.parseInternal(parser));
        assertThat(e.getMessage(), containsString("either [xpack] or [no_xpack] can be present, not both"));
    }

    public void testParseSkipSectionFeaturesNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "features:     [regex1,regex2,regex3]");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex1", "regex2", "regex3"));
        assertThat(skipSectionBuilder.skipReason, nullValue());
    }

    public void testParseSkipSectionBothFeatureAndVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            version:     " - 0.90.2"
            features:     regex
            reason:      Delete ignores the parent param""");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder.skipVersionRange, not(emptyOrNullString()));
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex"));
        assertThat(skipSectionBuilder.skipReason, equalTo("Delete ignores the parent param"));
    }

    public void testParseSkipSectionNoReason() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
               version: " - 0.90.2"
            """);

        Exception e = expectThrows(ParsingException.class, () -> PrerequisiteSection.parseInternal(parser));
        assertThat(e.getMessage(), is("reason is mandatory within skip version section"));
    }

    public void testParseSkipSectionNoVersionNorFeature() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
               reason:      Delete ignores the parent param
            """);

        Exception e = expectThrows(ParsingException.class, () -> PrerequisiteSection.parseInternal(parser));
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

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, hasSize(2));
        assertThat(skipSectionBuilder.skipOperatingSystems, contains("debian-9"));
        assertThat(skipSectionBuilder.skipReason, is("memory accounting broken, see gh#xyz"));
    }

    public void testParseSkipSectionOsListNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            features:    skip_os
            os:          [debian-9,windows-95,ms-dos]
            reason:      see gh#xyz
            """);

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, hasSize(1));
        assertThat(skipSectionBuilder.skipOperatingSystems, containsInAnyOrder("debian-9", "windows-95", "ms-dos"));
        assertThat(skipSectionBuilder.skipReason, is("see gh#xyz"));
    }

    public void testParseSkipSectionOsListTestFeaturesInRequires() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            - requires:
               test_runner_features: skip_os
               reason:      skip_os is needed for skip based on os
            - skip:
               os:          [debian-9,windows-95,ms-dos]
               reason:      see gh#xyz
            """);

        var skipSectionBuilder = PrerequisiteSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, hasSize(1));
        assertThat(skipSectionBuilder.skipOperatingSystems, containsInAnyOrder("debian-9", "windows-95", "ms-dos"));
        assertThat(skipSectionBuilder.skipReason, is("see gh#xyz"));

        assertThat(parser.currentToken(), equalTo(XContentParser.Token.END_ARRAY));
        assertThat(parser.nextToken(), nullValue());
    }

    public void testParseSkipSectionOsNoFeatureNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
               os:          debian-9
               reason:      memory accounting broken, see gh#xyz
            """);

        Exception e = expectThrows(ParsingException.class, () -> PrerequisiteSection.parseInternal(parser));
        assertThat(e.getMessage(), is("if os is specified, test runner feature [skip_os] must be set"));
    }

    public void testParseRequireSectionClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            cluster_features:          needed-feature
            reason:      test skipped when cluster lacks needed-feature
            """);

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseRequiresSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.requiredClusterFeatures, contains("needed-feature"));
        assertThat(skipSectionBuilder.requiresReason, is("test skipped when cluster lacks needed-feature"));
    }

    public void testParseSkipSectionClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            cluster_features:          undesired-feature
            reason:      test skipped when undesired-feature is present
            """);

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.skipClusterFeatures, contains("undesired-feature"));
        assertThat(skipSectionBuilder.skipReason, is("test skipped when undesired-feature is present"));
    }

    public void testParseRequireAndSkipSectionsClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            - requires:
               cluster_features: needed-feature
               reason:      test needs needed-feature to run
            - skip:
               cluster_features: undesired-feature
               reason:      test cannot run when undesired-feature are present
            """);

        var skipSectionBuilder = PrerequisiteSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.skipClusterFeatures, contains("undesired-feature"));
        assertThat(skipSectionBuilder.requiredClusterFeatures, contains("needed-feature"));
        assertThat(skipSectionBuilder.skipReason, is("test cannot run when undesired-feature are present"));
        assertThat(skipSectionBuilder.requiresReason, is("test needs needed-feature to run"));

        assertThat(parser.currentToken(), equalTo(XContentParser.Token.END_ARRAY));
        assertThat(parser.nextToken(), nullValue());
    }

    public void testParseRequireAndSkipSectionMultipleClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            - requires:
                cluster_features: [needed-feature-1, needed-feature-2]
                reason:      test needs some to run
            - skip:
                cluster_features:   [undesired-feature-1, undesired-feature-2]
                reason:      test cannot run when some are present
            """);

        var skipSectionBuilder = PrerequisiteSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.skipVersionRange, emptyOrNullString());
        assertThat(skipSectionBuilder.skipClusterFeatures, containsInAnyOrder("undesired-feature-1", "undesired-feature-2"));
        assertThat(skipSectionBuilder.requiredClusterFeatures, containsInAnyOrder("needed-feature-1", "needed-feature-2"));
        assertThat(skipSectionBuilder.skipReason, is("test cannot run when some are present"));
        assertThat(skipSectionBuilder.requiresReason, is("test needs some to run"));

        assertThat(parser.currentToken(), equalTo(XContentParser.Token.END_ARRAY));
        assertThat(parser.nextToken(), nullValue());
    }

    public void testParseSameRequireAndSkipClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            - requires:
               cluster_features: some-feature
               reason: test needs some-feature to run
            - skip:
               cluster_features: some-feature
               reason: test cannot run with some-feature
            """);

        var e = expectThrows(ParsingException.class, () -> PrerequisiteSection.parseInternal(parser));
        assertThat(e.getMessage(), is("a cluster feature can be specified either in [requires] or [skip], not both"));

        assertThat(parser.currentToken(), equalTo(XContentParser.Token.END_ARRAY));
        assertThat(parser.nextToken(), nullValue());
    }

    public void testSkipClusterFeaturesAllRequiredMatch() {
        PrerequisiteSection section = new PrerequisiteSection(
            emptyList(),
            "foobar",
            List.of(Prerequisites.requireClusterFeatures(Set.of("required-feature-1", "required-feature-2"))),
            "foobar",
            emptyList()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1")).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2")).thenReturn(true);

        assertFalse(section.skipCriteriaMet(mockContext));
        assertTrue(section.requiresCriteriaMet(mockContext));
    }

    public void testSkipClusterFeaturesSomeRequiredMatch() {
        PrerequisiteSection section = new PrerequisiteSection(
            emptyList(),
            "foobar",
            List.of(Prerequisites.requireClusterFeatures(Set.of("required-feature-1", "required-feature-2"))),
            "foobar",
            emptyList()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1")).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2")).thenReturn(false);

        assertFalse(section.skipCriteriaMet(mockContext));
        assertFalse(section.requiresCriteriaMet(mockContext));
    }

    public void testSkipClusterFeaturesSomeToSkipMatch() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnClusterFeatures(Set.of("undesired-feature-1", "undesired-feature-2"))),
            "foobar",
            emptyList(),
            "foobar",
            emptyList()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("undesired-feature-1")).thenReturn(true);

        assertTrue(section.skipCriteriaMet(mockContext));
    }

    public void testSkipClusterFeaturesNoneToSkipMatch() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnClusterFeatures(Set.of("undesired-feature-1", "undesired-feature-2"))),
            "foobar",
            emptyList(),
            "foobar",
            emptyList()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        assertFalse(section.skipCriteriaMet(mockContext));
    }

    public void testSkipClusterFeaturesAllRequiredSomeToSkipMatch() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnClusterFeatures(Set.of("undesired-feature-1", "undesired-feature-2"))),
            "foobar",
            List.of(Prerequisites.requireClusterFeatures(Set.of("required-feature-1", "required-feature-2"))),
            "foobar",
            emptyList()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1")).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2")).thenReturn(true);
        when(mockContext.clusterHasFeature("undesired-feature-1")).thenReturn(true);

        assertTrue(section.skipCriteriaMet(mockContext));
        assertTrue(section.requiresCriteriaMet(mockContext));
    }

    public void testSkipClusterFeaturesAllRequiredNoneToSkipMatch() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnClusterFeatures(Set.of("undesired-feature-1", "undesired-feature-2"))),
            "foobar",
            List.of(Prerequisites.requireClusterFeatures(Set.of("required-feature-1", "required-feature-2"))),
            "foobar",
            emptyList()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1")).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2")).thenReturn(true);

        assertFalse(section.skipCriteriaMet(mockContext));
        assertTrue(section.requiresCriteriaMet(mockContext));
    }

    public void evaluateEmpty() {
        var section = new PrerequisiteSection(List.of(), "unsupported", List.of(), "required", List.of());

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        section.evaluate(mockContext, "TEST");
    }

    public void evaluateRequiresCriteriaTrue() {
        var section = new PrerequisiteSection(List.of(), "unsupported", List.of(Prerequisites.TRUE), "required", List.of());

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        section.evaluate(mockContext, "TEST");
    }

    public void evaluateSkipCriteriaFalse() {
        var section = new PrerequisiteSection(List.of(Prerequisites.FALSE), "unsupported", List.of(), "required", List.of());

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        section.evaluate(mockContext, "TEST");
    }

    public void evaluateRequiresCriteriaFalse() {
        var section = new PrerequisiteSection(
            List.of(Prerequisites.FALSE),
            "unsupported",
            List.of(Prerequisites.FALSE),
            "required",
            List.of()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        var e = expectThrows(AssumptionViolatedException.class, () -> section.evaluate(mockContext, "TEST"));
        assertThat(e.getMessage(), equalTo("[TEST] skipped, reason: [required]"));
    }

    public void evaluateSkipCriteriaTrue() {
        var section = new PrerequisiteSection(
            List.of(Prerequisites.TRUE),
            "unsupported",
            List.of(Prerequisites.TRUE),
            "required",
            List.of()
        );

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        var e = expectThrows(AssumptionViolatedException.class, () -> section.evaluate(mockContext, "TEST"));
        assertThat(e.getMessage(), equalTo("[TEST] skipped, reason: [unsupported]"));
    }
}
