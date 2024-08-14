/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.section.PrerequisiteSection.CapabilitiesCheck;
import org.elasticsearch.test.rest.yaml.section.PrerequisiteSection.KnownIssue;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrerequisiteSectionTests extends AbstractClientYamlTestFragmentParserTestCase {

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

    public void testSkipAwaitsFix() {
        PrerequisiteSection section = new PrerequisiteSection.PrerequisiteSectionBuilder().skipIfAwaitsFix("bugurl").build();
        assertTrue(section.skipCriteriaMet(mock(ClientYamlTestExecutionContext.class)));
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
        PrerequisiteSection section = new PrerequisiteSection(List.of(), "unsupported", emptyList(), "required", singletonList("warnings"));
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

    public void testParseSkipSectionFeature() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "features:     regex");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex"));
        assertThat(skipSectionBuilder.skipReason, nullValue());
        assertThat(skipSectionBuilder.xpackRequired, is(PrerequisiteSection.PrerequisiteSectionBuilder.XPackRequired.NOT_SPECIFIED));
    }

    public void testParseXPackFeature() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, "features:     xpack");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, empty());
        assertThat(skipSectionBuilder.skipReason, nullValue());
        assertThat(skipSectionBuilder.xpackRequired, is(PrerequisiteSection.PrerequisiteSectionBuilder.XPackRequired.YES));
    }

    public void testParseNoXPackFeature() throws IOException {
        parser = createParser(YamlXContent.yamlXContent, "features:     no_xpack");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
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

    public void testParseSkipSectionFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "features:     [regex1,regex2,regex3]");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex1", "regex2", "regex3"));
        assertThat(skipSectionBuilder.skipReason, nullValue());
    }

    public void testParseSkipSectionBothFeatureAndClusterFeature() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            cluster_features: feature1
            features: regex
            reason: Delete ignores the parent param""");

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder.skipClusterFeatures, contains("feature1"));
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, contains("regex"));
        assertThat(skipSectionBuilder.skipReason, equalTo("Delete ignores the parent param"));
    }

    public void testParseSkipSectionAwaitsFix() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
              awaits_fix: "bugurl"
            """);

        var skipSectionBuilder = PrerequisiteSection.parseInternal(parser);
        assertThat(skipSectionBuilder.skipAwaitsFix, is("bugurl"));
    }

    public void testParseSkipSectionKnownIssues() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
              reason: "skip known bugs"
              known_issues:
              - cluster_feature: feature1
                fixed_by: featureFix1
              - cluster_feature: feature2
                fixed_by: featureFix2""");

        var skipSectionBuilder = PrerequisiteSection.parseInternal(parser);
        assertThat(skipSectionBuilder.skipReason, is("skip known bugs"));
        assertThat(
            skipSectionBuilder.skipKnownIssues,
            contains(
                new KnownIssue("feature1", "featureFix1"), //
                new KnownIssue("feature2", "featureFix2")
            )
        );
    }

    public void testParseSkipSectionIncompleteKnownIssues() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
              reason: "skip known bugs"
              known_issues:
              - cluster_feature: feature1""");

        Exception e = expectThrows(ParsingException.class, () -> PrerequisiteSection.parseInternal(parser));
        parser = null; // parser is not fully consumed, prevent validation
        assertThat(
            e.getMessage(),
            is(
                oneOf(
                    ("Expected all of [cluster_feature, fixed_by], but got [cluster_feature]"),
                    ("Expected all of [fixed_by, cluster_feature], but got [cluster_feature]")
                )
            )
        );
    }

    public void testParseSkipSectionNoReason() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
               cluster_features: "feature"
            """);

        Exception e = expectThrows(ParsingException.class, () -> PrerequisiteSection.parseInternal(parser));
        assertThat(e.getMessage(), is("reason is mandatory within this skip section"));
    }

    public void testParseSkipSectionNoVersionNorFeature() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            skip:
               reason:      Delete ignores the parent param
            """);

        Exception e = expectThrows(ParsingException.class, () -> PrerequisiteSection.parseInternal(parser));
        assertThat(e.getMessage(), is("at least one predicate is mandatory within a skip or requires section"));
    }

    public void testParseSkipSectionOs() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            features:    ["skip_os", "some_feature"]
            os:          debian-9
            reason:      memory accounting broken, see gh#xyz
            """);

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(skipSectionBuilder.requiredYamlRunnerFeatures, hasSize(2));
        assertThat(skipSectionBuilder.skipOperatingSystems, contains("debian-9"));
        assertThat(skipSectionBuilder.skipReason, is("memory accounting broken, see gh#xyz"));
    }

    public void testParseSkipSectionOsList() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            features:    skip_os
            os:          [debian-9,windows-95,ms-dos]
            reason:      see gh#xyz
            """);

        var skipSectionBuilder = new PrerequisiteSection.PrerequisiteSectionBuilder();
        PrerequisiteSection.parseSkipSection(parser, skipSectionBuilder);
        assertThat(skipSectionBuilder, notNullValue());
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
        assertThat(skipSectionBuilder.skipClusterFeatures, contains("undesired-feature"));
        assertThat(skipSectionBuilder.requiredClusterFeatures, contains("needed-feature"));
        assertThat(skipSectionBuilder.skipReason, is("test cannot run when undesired-feature are present"));
        assertThat(skipSectionBuilder.requiresReason, is("test needs needed-feature to run"));

        assertThat(parser.currentToken(), equalTo(XContentParser.Token.END_ARRAY));
        assertThat(parser.nextToken(), nullValue());
    }

    public void testParseRequireAndSkipSectionsCapabilities() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            - requires:
               capabilities:
                 - path: /a
                 - method: POST
                   path: /b
                   parameters: [param1, param2]
                 - method: PUT
                   path: /c
                   capabilities: [a, b, c]
               reason: required to run test
            - skip:
               capabilities:
                 - path: /d
                   parameters: param1
                   capabilities: a
               reason: undesired if supported
            """);

        var skipSectionBuilder = PrerequisiteSection.parseInternal(parser);
        assertThat(skipSectionBuilder, notNullValue());
        assertThat(
            skipSectionBuilder.requiredCapabilities,
            contains(
                new CapabilitiesCheck("GET", "/a", null, null),
                new CapabilitiesCheck("POST", "/b", "param1,param2", null),
                new CapabilitiesCheck("PUT", "/c", null, "a,b,c")
            )
        );
        assertThat(skipSectionBuilder.skipCapabilities, contains(new CapabilitiesCheck("GET", "/d", "param1", "a")));

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
        when(mockContext.clusterHasFeature("required-feature-1", false)).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2", false)).thenReturn(true);

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
        when(mockContext.clusterHasFeature("required-feature-1", false)).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2", false)).thenReturn(false);

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
        when(mockContext.clusterHasFeature("undesired-feature-1", true)).thenReturn(true);

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
        assertFalse(section.hasCapabilitiesCheck());

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
        assertFalse(section.hasCapabilitiesCheck());

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1", false)).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2", false)).thenReturn(true);
        when(mockContext.clusterHasFeature("undesired-feature-1", true)).thenReturn(true);

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
        assertFalse(section.hasCapabilitiesCheck());

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        when(mockContext.clusterHasFeature("required-feature-1", false)).thenReturn(true);
        when(mockContext.clusterHasFeature("required-feature-2", false)).thenReturn(true);

        assertFalse(section.skipCriteriaMet(mockContext));
        assertTrue(section.requiresCriteriaMet(mockContext));
    }

    public void testSkipKnownIssue() {
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipOnKnownIssue(List.of(new KnownIssue("bug1", "fix1"), new KnownIssue("bug2", "fix2")))),
            "foobar",
            emptyList(),
            "foobar",
            emptyList()
        );
        assertFalse(section.hasCapabilitiesCheck());

        var mockContext = mock(ClientYamlTestExecutionContext.class);
        assertFalse(section.skipCriteriaMet(mockContext));

        when(mockContext.clusterHasFeature("bug1", true)).thenReturn(true);
        assertTrue(section.skipCriteriaMet(mockContext));

        when(mockContext.clusterHasFeature("fix1", false)).thenReturn(true);
        assertFalse(section.skipCriteriaMet(mockContext));

        when(mockContext.clusterHasFeature("bug2", true)).thenReturn(true);
        assertTrue(section.skipCriteriaMet(mockContext));

        when(mockContext.clusterHasFeature("fix2", false)).thenReturn(true);
        assertFalse(section.skipCriteriaMet(mockContext));
    }

    public void testEvaluateCapabilities() {
        List<CapabilitiesCheck> skipCapabilities = List.of(
            new CapabilitiesCheck("GET", "/s", null, "c1,c2"),
            new CapabilitiesCheck("GET", "/s", "p1,p2", "c1")
        );
        List<CapabilitiesCheck> requiredCapabilities = List.of(
            new CapabilitiesCheck("GET", "/r", null, null),
            new CapabilitiesCheck("GET", "/r", "p1", null)
        );
        PrerequisiteSection section = new PrerequisiteSection(
            List.of(Prerequisites.skipCapabilities(skipCapabilities)),
            "skip",
            List.of(Prerequisites.requireCapabilities(requiredCapabilities)),
            "required",
            emptyList()
        );
        assertTrue(section.hasCapabilitiesCheck());
        var context = mock(ClientYamlTestExecutionContext.class);

        // when the capabilities API is unavailable:
        assertTrue(section.skipCriteriaMet(context)); // always skip if unavailable
        assertFalse(section.requiresCriteriaMet(context)); // always fail requirements / skip if unavailable

        when(context.clusterHasCapabilities(anyString(), anyString(), any(), any(), anyBoolean())).thenReturn(Optional.of(FALSE));
        assertFalse(section.skipCriteriaMet(context));
        assertFalse(section.requiresCriteriaMet(context));

        when(context.clusterHasCapabilities("GET", "/s", null, "c1,c2", true)).thenReturn(Optional.of(TRUE));
        assertTrue(section.skipCriteriaMet(context));

        when(context.clusterHasCapabilities("GET", "/r", null, null, false)).thenReturn(Optional.of(TRUE));
        assertFalse(section.requiresCriteriaMet(context));

        when(context.clusterHasCapabilities("GET", "/r", "p1", null, false)).thenReturn(Optional.of(TRUE));
        assertTrue(section.requiresCriteriaMet(context));
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
