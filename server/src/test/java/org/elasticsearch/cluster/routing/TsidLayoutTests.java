/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TsidLayoutTests extends ESTestCase {

    private static final List<String> OTEL = List.of("resource.attributes.*");
    private static final List<String> PROMETHEUS = List.of("labels.job", "labels.instance");

    private static IndexMetadata indexMetadata(IndexVersion version, @Nullable List<String> prefixedDimensions) {
        Settings.Builder settings = settings(version).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim.*");
        if (prefixedDimensions != null) {
            settings.putList(IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.getKey(), prefixedDimensions);
        }
        return IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
    }

    public void testForIndexReadsTheSetting() {
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH);
        assertThat(TsidLayout.forIndex(indexMetadata(version, null)), equalTo(new TsidLayout(version, List.of())));
        assertThat(TsidLayout.forIndex(indexMetadata(version, OTEL)), equalTo(new TsidLayout(version, OTEL)));
        assertThat(TsidLayout.forIndex(indexMetadata(version, PROMETHEUS)), equalTo(new TsidLayout(version, PROMETHEUS)));
        assertThat(TsidLayout.forIndex(indexMetadata(version, List.of())), equalTo(new TsidLayout(version, List.of())));
    }

    /** The setting is off by default: the index templates that know their dimension layout opt in. */
    public void testForVersionAssumesDefaultSettings() {
        IndexVersion version = IndexVersionUtils.randomVersion();
        assertThat(TsidLayout.forVersion(version), equalTo(TsidLayout.forIndex(indexMetadata(version, null))));
        assertThat(TsidLayout.forVersion(version).prefixedDimensions(), empty());
        assertThat(TsidLayout.forVersion(version).prefixedDimensionMatcher(), nullValue());
    }

    public void testPrefixedDimensionMatcher() {
        IndexVersion current = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH);
        Predicate<String> otel = new TsidLayout(current, OTEL).prefixedDimensionMatcher();
        assertThat(otel, notNullValue());
        assertTrue(otel.test("resource.attributes.host.name"));
        assertFalse(otel.test("resource.attributes"));
        assertFalse(otel.test("attributes.host.name"));
        assertFalse(otel.test("scope.attributes.host.name"));

        // Exact paths match themselves only: no implicit prefix matching.
        Predicate<String> prometheus = new TsidLayout(current, PROMETHEUS).prefixedDimensionMatcher();
        assertThat(prometheus, notNullValue());
        assertTrue(prometheus.test("labels.job"));
        assertTrue(prometheus.test("labels.instance"));
        assertFalse(prometheus.test("labels.job_name"));
        assertFalse(prometheus.test("labels.instance.id"));
        assertFalse(prometheus.test("job"));

        // Patterns and exact paths mix, and a wildcard may sit anywhere in a pattern.
        Predicate<String> mixed = new TsidLayout(current, List.of("labels.job", "resource.*.name", "*.region")).prefixedDimensionMatcher();
        assertTrue(mixed.test("labels.job"));
        assertTrue(mixed.test("resource.attributes.host.name"));
        assertTrue(mixed.test("attributes.cloud.region"));
        assertFalse(mixed.test("labels.instance"));
        assertFalse(mixed.test("resource.attributes.host.id"));

        assertThat(new TsidLayout(current, List.of()).prefixedDimensionMatcher(), nullValue());

        IndexVersion previous = IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH);
        assertThat(TsidLayout.forVersion(previous).prefixedDimensionMatcher(), nullValue());
        assertThat(new TsidLayout(previous, OTEL).prefixedDimensionMatcher(), nullValue());
    }

    public void testSingleBytePrefix() {
        assertTrue(TsidLayout.forVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE).singleBytePrefix());
        assertTrue(TsidLayout.forVersion(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH).singleBytePrefix());
        assertFalse(
            TsidLayout.forVersion(IndexVersionUtils.getPreviousVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG))
                .singleBytePrefix()
        );
    }

    public void testEqualsAndHashCode() {
        IndexVersion version = IndexVersionUtils.randomVersion();
        assertThat(new TsidLayout(version, OTEL), equalTo(new TsidLayout(version, List.of("resource.attributes.*"))));
        assertThat(new TsidLayout(version, OTEL).hashCode(), equalTo(new TsidLayout(version, List.of("resource.attributes.*")).hashCode()));
        assertThat(new TsidLayout(version, OTEL), not(equalTo(new TsidLayout(version, PROMETHEUS))));
        assertThat(new TsidLayout(version, OTEL), not(equalTo(new TsidLayout(version, List.of()))));
        IndexVersion other = randomValueOtherThan(version, IndexVersionUtils::randomVersion);
        assertThat(new TsidLayout(version, OTEL), not(equalTo(new TsidLayout(other, OTEL))));
    }

    public void testRejectsNulls() {
        expectThrows(NullPointerException.class, () -> new TsidLayout(null, OTEL));
        expectThrows(NullPointerException.class, () -> new TsidLayout(IndexVersion.current(), null));
    }

    public void testSettingIsFinal() {
        assertTrue(IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.isFinal());
    }

    public void testSettingRejectsEmptyPatterns() {
        String key = IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.getKey();
        List<Settings> invalid = List.of(
            Settings.builder().put(key, " ").build(),
            Settings.builder().put(key, ",labels.job").build(),
            Settings.builder().put(key, "labels.job,,labels.instance").build(),
            Settings.builder().putList(key, "resource.attributes.*", "").build(),
            Settings.builder().putList(key, "  ").build()
        );
        for (Settings settings : invalid) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.get(settings)
            );
            assertThat(e.getMessage(), containsString("must not contain empty patterns"));
        }
        // An empty value, however spelled, is the empty list and disables the hash.
        assertThat(IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.get(Settings.builder().put(key, "").build()), empty());
        assertThat(IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.get(Settings.builder().putList(key, List.of()).build()), empty());
        assertThat(
            IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.get(Settings.builder().put(key, "labels.job, labels.instance").build()),
            equalTo(PROMETHEUS)
        );
    }

    private static ProjectMetadata projectWithTemplates() {
        Settings otelSettings = Settings.builder().putList(IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.getKey(), OTEL).build();
        Settings prometheusSettings = Settings.builder()
            .putList(IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.getKey(), PROMETHEUS)
            .build();
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            // The setting comes from a component template, as in the OTel templates.
            .put("metrics-otel@settings", new ComponentTemplate(new Template(otelSettings, null, null), null, null))
            .put(
                "metrics-otel@template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("metrics-*.otel-*"))
                    .componentTemplates(List.of("metrics-otel@settings"))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .priority(120L)
                    .build()
            )
            // The setting sits in the index template itself, and the template outranks the OTel one for its pattern.
            .put(
                "metrics-prometheus@template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("metrics-*.prometheus-*"))
                    .template(new Template(prometheusSettings, null, null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .priority(140L)
                    .build()
            )
            // A template that does not set it.
            .put(
                "metrics-generic@template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("metrics-*"))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .priority(100L)
                    .build()
            )
            .build();
    }

    /** Before the data stream exists, the layout comes from the index template that will create it, so the first batch matches. */
    public void testForNewIndexResolvesTheTemplateSetting() {
        ProjectMetadata project = projectWithTemplates();
        assertThat(TsidLayout.forNewIndex(project, "metrics-generic.otel-default"), equalTo(new TsidLayout(IndexVersion.current(), OTEL)));
        assertThat(
            TsidLayout.forNewIndex(project, "metrics-node.prometheus-default"),
            equalTo(new TsidLayout(IndexVersion.current(), PROMETHEUS))
        );
        assertThat(TsidLayout.forNewIndex(project, "metrics-generic-default"), equalTo(TsidLayout.forVersion(IndexVersion.current())));
        assertThat(TsidLayout.forNewIndex(project, "logs-generic-default"), equalTo(TsidLayout.forVersion(IndexVersion.current())));
    }
}
