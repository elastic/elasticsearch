/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class IndexDeprecationChecksTests extends ESTestCase {
    public void testOldIndicesCheck() {
        IndexVersion createdWith = IndexVersion.fromId(1000099);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(createdWith))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Old index with a compatibility version < 7.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/" + "breaking-changes-8.0.html",
            "This index has version: " + createdWith,
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetadata));
        assertEquals(singletonList(expected), issues);
    }

    public void testTranslogRetentionSettings() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
        settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetadata));
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "translog retention settings are ignored",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-translog.html",
                    "translog retention settings [index.translog.retention.size] and [index.translog.retention.age] are ignored "
                        + "because translog is no longer used in peer recoveries with soft-deletes enabled (default in 7.0 or later)",
                    false,
                    DeprecationIssue.createMetaMapForRemovableSettings(
                        List.of(
                            IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(),
                            IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey()
                        )
                    )
                )
            )
        );
    }

    public void testDefaultTranslogRetentionSettings() {
        Settings.Builder settings = settings(Version.CURRENT);
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
            settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false);
        }
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetadata));
        assertThat(issues, empty());
    }

    public void testIndexDataPathSetting() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(IndexMetadata.INDEX_DATA_PATH_SETTING.getKey(), createTempDir());
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetadata));
        final String expectedUrl =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/breaking-changes-7.13.html#deprecate-shared-data-path-setting";
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "setting [index.data_path] is deprecated and will be removed in a future version",
                    expectedUrl,
                    "Found index data path configured. Discontinue use of this setting.",
                    false,
                    null
                )
            )
        );
    }

    public void testSimpleFSSetting() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "simplefs");
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetadata));
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "[simplefs] is deprecated and will be removed in future versions",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html",
                    "[simplefs] is deprecated and will be removed in 8.0. Use [niofs] or other file systems instead. "
                        + "Elasticsearch 7.15 or later uses [niofs] for the [simplefs] store type "
                        + "as it offers superior or equivalent performance to [simplefs].",
                    false,
                    null
                )
            )
        );
    }

    public void testFrozenIndex() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(FrozenEngine.INDEX_FROZEN.getKey(), true);
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetadata));
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "index [test] is a frozen index. The frozen indices feature is deprecated and will be removed in a future version",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/frozen-indices.html",
                    "Frozen indices no longer offer any advantages. Consider cold or frozen tiers in place of frozen indices.",
                    false,
                    null
                )
            )
        );
    }

    public void testCamelCaseDeprecation() throws IOException {
        String simpleMapping = "{\n\"_doc\": {"
            + "\"properties\" : {\n"
            + "   \"date_time_field\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"strictDateOptionalTime\"\n"
            + "       }\n"
            + "   }"
            + "} }";

        IndexMetadata simpleIndex = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(Version.V_7_0_0))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putMapping(simpleMapping)
            .build();

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Date fields use deprecated camel case formats",
            "https://ela.st/es-deprecation-7-camel-case-format",
            "Convert [date_time_field] format [strictDateOptionalTime] "
                + "which contains deprecated camel case to snake case. [strictDateOptionalTime] to [strict_date_optional_time].",
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
        assertThat(issues, hasItem(expected));
    }
}
