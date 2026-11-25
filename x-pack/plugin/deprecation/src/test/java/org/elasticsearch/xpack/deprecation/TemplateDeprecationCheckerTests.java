/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class TemplateDeprecationCheckerTests extends ESTestCase {

    private final TemplateDeprecationChecker checker = new TemplateDeprecationChecker();

    public void testCheckSourceModeInComponentTemplates() throws IOException {
        Template template = Template.builder().mappings(CompressedXContent.fromJSON("""
            { "_doc": { "_source": { "mode": "stored"} } }""")).build();
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());

        Template template2 = Template.builder().mappings(CompressedXContent.fromJSON("""
            { "_doc": { "_source": { "enabled": false} } }""")).build();
        ComponentTemplate componentTemplate2 = new ComponentTemplate(template2, 1L, new HashMap<>());

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .componentTemplates(
                Map.of("my-template-1", componentTemplate, "my-template-2", componentTemplate, "my-template-3", componentTemplate2)
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(project);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            SourceFieldMapper.DEPRECATION_WARNING_TITLE,
            "https://ela.st/migrate-source-mode",
            SourceFieldMapper.DEPRECATION_WARNING,
            false,
            null
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }

    public void testCheckLegacyTiersInComponentTemplates() {
        String setting = "index.routing.allocation." + randomFrom("include", "require", "exclude") + ".data";
        Template template = Template.builder().settings(Settings.builder().put(setting, "hot").build()).build();
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());

        Template template2 = Template.builder()
            .settings(Settings.builder().put("index.routing.allocation.require.data", randomAlphaOfLength(10)).build())
            .build();
        ComponentTemplate componentTemplate2 = new ComponentTemplate(template2, 1L, new HashMap<>());

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .componentTemplates(
                Map.of("my-template-1", componentTemplate, "my-template-2", componentTemplate, "my-template-3", componentTemplate2)
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(project);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your component templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }

    public void testCheckLegacyTierSettings() {
        String setting = "index.routing.allocation." + randomFrom("include", "require", "exclude") + ".data";
        Template template = Template.builder().settings(Settings.builder().put(setting, "hot").build()).build();

        Template template2 = Template.builder()
            .settings(Settings.builder().put("index.routing.allocation.require.data", randomAlphaOfLength(10)).build())
            .build();

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(
                Map.of(
                    "my-template-1",
                    ComposableIndexTemplate.builder().template(template).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                    "my-template-2",
                    ComposableIndexTemplate.builder().template(template).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                    "my-template-3",
                    ComposableIndexTemplate.builder().template(template2).indexPatterns(List.of(randomAlphaOfLength(10))).build()
                )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(project);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your index templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }

    public void testCheckMinHashParamBounds() {
        // Template settings are stored with the "index." prefix.
        Template big = Template.builder()
            .settings(
                Settings.builder()
                    .put("index.analysis.filter.big.type", "min_hash")
                    .put("index.analysis.filter.big.hash_count", 1)
                    .put("index.analysis.filter.big.bucket_count", 512)
                    .put("index.analysis.filter.big.hash_set_size", 200)
                    .put("index.analysis.filter.small.type", "min_hash")
                    .put("index.analysis.filter.small.hash_set_size", 20)
                    .put("index.analysis.filter.other.type", "lowercase")
            )
            .build();
        Template small = Template.builder()
            .settings(
                Settings.builder().put("index.analysis.filter.small.type", "min_hash").put("index.analysis.filter.small.hash_set_size", 20)
            )
            .build();

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .componentTemplates(
                Map.of(
                    "big-component",
                    new ComponentTemplate(big, 1L, new HashMap<>()),
                    "small-component",
                    new ComponentTemplate(small, 1L, new HashMap<>())
                )
            )
            .indexTemplates(
                Map.of(
                    "big-index",
                    ComposableIndexTemplate.builder().template(big).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                    "small-index",
                    ComposableIndexTemplate.builder().template(small).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                    "inherits-index",
                    ComposableIndexTemplate.builder()
                        .template(small)
                        .componentTemplates(List.of("big-component"))
                        .indexPatterns(List.of(randomAlphaOfLength(10)))
                        .build()
                )
            )
            .put(IndexTemplateMetadata.builder("big-legacy").patterns(List.of(randomAlphaOfLength(10))).settings(big.settings()))
            .put(IndexTemplateMetadata.builder("small-legacy").patterns(List.of(randomAlphaOfLength(10))).settings(small.settings()))
            .build();

        Function<String, DeprecationIssue> expectedIssue = templateKind -> new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "min_hash token filter parameters exceed the limits for new indices",
            "https://www.elastic.co/docs/reference/text-analysis/analysis-minhash-tokenfilter",
            "The following [min_hash] filters in this "
                + templateKind
                + " can't be used on new indices: [big]: The product of hash_count [1], bucket_count [512], and hash_set_size [200] "
                + "is [102400], which exceeds the limit [100000]. Creating an index from this "
                + templateKind
                + " fails. Lower hash_count, bucket_count, or hash_set_size in the template; "
                + "note that this changes the hashes the filter produces.",
            false,
            null
        );

        // Only the filter that exceeds the bounds is reported, not one within them or a filter of another type.
        Map<String, List<DeprecationIssue>> issues = checker.check(project);
        assertThat(issues.get("big-component"), contains(expectedIssue.apply("component template")));
        assertThat(issues.get("big-index"), contains(expectedIssue.apply("index template")));
        assertThat(issues.containsKey("small-component"), is(false));
        assertThat(issues.containsKey("small-index"), is(false));
        // The index template's own settings are within the bounds, but it inherits the violation from its component template.
        assertThat(issues.get("inherits-index"), contains(expectedIssue.apply("index template")));
        assertThat(issues.get("big-legacy"), contains(expectedIssue.apply("legacy index template")));
        assertThat(issues.containsKey("small-legacy"), is(false));
    }

    public void testComponentAndComposableTemplateWithSameName() {
        String setting = "index.routing.allocation." + randomFrom("include", "require", "exclude") + ".data";
        Template template = Template.builder().settings(Settings.builder().put(setting, "hot").build()).build();

        Template template2 = Template.builder()
            .settings(Settings.builder().put("index.routing.allocation.require.data", randomAlphaOfLength(10)).build())
            .build();

        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .componentTemplates(Map.of("my-template-1", componentTemplate))
            .indexTemplates(
                Map.of(
                    "my-template-1",
                    ComposableIndexTemplate.builder().template(template).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                    "my-template-2",
                    ComposableIndexTemplate.builder().template(template).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                    "my-template-3",
                    ComposableIndexTemplate.builder().template(template2).indexPatterns(List.of(randomAlphaOfLength(10))).build()
                )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(project);
        final DeprecationIssue expectedIndexTemplateIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your index templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        final DeprecationIssue expectedComponentTemplateIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your component templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expectedIndexTemplateIssue));
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expectedComponentTemplateIssue));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expectedIndexTemplateIssue));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }
}
