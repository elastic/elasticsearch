/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingSlowLog;
import org.elasticsearch.index.SearchSlowLog;
import org.elasticsearch.index.SlowLogLevel;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_REQUIRE_SETTING;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.IndexDeprecationChecks.JODA_TIME_DEPRECATION_DETAILS_SUFFIX;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class IndexDeprecationChecksTests extends ESTestCase {
    public void testOldIndicesCheck() {
        Version createdWith = VersionUtils.randomVersionBetween(
            random(),
            Version.V_6_0_0,
            VersionUtils.getPreviousVersion(Version.V_7_0_0)
        );
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(createdWith))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Index created before 7.0",
            "https://ela.st/es-deprecation-7-reindex",
            "This index was created with version "
                + createdWith
                + " and is not compatible with 8.0. Reindex or remove the index before "
                + "upgrading.",
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testTooManyFieldsCheck() throws IOException {
        String simpleMapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"some_field\": {\n"
            + "      \"type\": \"text\"\n"
            + "    },\n"
            + "    \"other_field\": {\n"
            + "      \"type\": \"text\",\n"
            + "      \"properties\": {\n"
            + "        \"raw\": {\"type\": \"keyword\"}\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        IndexMetadata simpleIndex = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(Version.V_7_0_0))
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 100))
            .putMapping("_doc", simpleMapping)
            .build();
        List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, simpleIndex)
        );
        assertEquals(0, noIssues.size());

        // Test that it catches having too many fields
        int fieldCount = randomIntBetween(1025, 10_000); // 10_000 is arbitrary

        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                addRandomFields(fieldCount, mappingBuilder);
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();

        IndexMetadata tooManyFieldsIndex = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(Version.V_7_0_0))
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 100))
            .putMapping("_doc", Strings.toString(mappingBuilder))
            .build();
        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Number of fields exceeds automatic field expansion limit",
            "https://ela.st/es-deprecation-7-number-of-auto-expanded-fields",
            "This index has "
                + fieldCount
                + " fields, which exceeds the automatic field expansion limit (1024). Set "
                + IndexSettings.DEFAULT_FIELD_SETTING.getKey()
                + " to prevent queries that support automatic field expansion from failing "
                + "if no fields are specified. Otherwise, you must explicitly specify fields in all query_string, simple_query_string, and "
                + "multi_match queries.",
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, tooManyFieldsIndex)
        );
        assertEquals(singletonList(expected), issues);

        // Check that it's okay to have too many fields as long as `index.query.default_field` is set
        IndexMetadata tooManyFieldsOk = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(Version.V_7_0_0).put(IndexSettings.DEFAULT_FIELD_SETTING.getKey(), randomAlphaOfLength(5)))
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 100))
            .putMapping("_doc", Strings.toString(mappingBuilder))
            .build();
        List<DeprecationIssue> withDefaultFieldIssues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, tooManyFieldsOk)
        );
        assertEquals(0, withDefaultFieldIssues.size());
    }

    public void testChainedMultiFields() throws IOException {
        XContentBuilder xContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("invalid-field")
            .field("type", "keyword")
            .startObject("fields")
            .startObject("sub-field")
            .field("type", "keyword")
            .startObject("fields")
            .startObject("sub-sub-field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .startObject("valid-field")
            .field("type", "keyword")
            .startObject("fields")
            .startObject("sub-field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        String mapping = BytesReference.bytes(xContent).utf8ToString();

        IndexMetadata simpleIndex = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(Version.V_7_3_0))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putMapping("_doc", mapping)
            .build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, simpleIndex)
        );
        assertEquals(1, issues.size());

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining multi-fields within multi-fields is deprecated",
            "https://ela.st/es-deprecation-7-chained-multi-fields",
            "Remove chained multi-fields from the \"invalid-field\" mapping. Multi-fields within multi-fields are not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testDefinedPatternsDoNotWarn() throws IOException {
        String simpleMapping = "{\n"
            + "\"properties\" : {\n"
            + "   \"date_time_field_Y\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"strictWeekyearWeek\"\n"
            + "       }\n"
            + "   }"
            + "}";
        IndexMetadata simpleIndex = createV6Index(simpleMapping);

        DeprecationIssue issue = IndexDeprecationChecks.deprecatedDateTimeFormat(simpleIndex);
        assertNull(issue);
    }

    public void testMigratedPatterns() throws IOException {
        String simpleMapping = "{\n"
            + "\"properties\" : {\n"
            + "   \"date_time_field_Y\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"8MM-YYYY\"\n"
            + "       }\n"
            + "   }"
            + "}";
        IndexMetadata simpleIndex = createV6Index(simpleMapping);

        DeprecationIssue issue = IndexDeprecationChecks.deprecatedDateTimeFormat(simpleIndex);
        assertNull(issue);
    }

    public void testMultipleWarningsOnCombinedPattern() throws IOException {
        String simpleMapping = "{\n"
            + "\"properties\" : {\n"
            + "   \"date_time_field_Y\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"dd-CC||MM-YYYY\"\n"
            + "       }\n"
            + "   }"
            + "}";
        IndexMetadata simpleIndex = createV6Index(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Date fields use deprecated Joda time formats",
            "https://ela.st/es-deprecation-7-java-time",
            "Convert [date_time_field_Y] format dd-CC||MM-YYYY to java.time." + JODA_TIME_DEPRECATION_DETAILS_SUFFIX,
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, simpleIndex)
        );
        assertThat(issues, hasItem(expected));
    }

    public void testDuplicateWarningsOnCombinedPattern() throws IOException {
        String simpleMapping = "{\n"
            + "\"properties\" : {\n"
            + "   \"date_time_field_Y\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"dd-YYYY||MM-YYYY\"\n"
            + "       }\n"
            + "   }"
            + "}";
        IndexMetadata simpleIndex = createV6Index(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Date fields use deprecated Joda time formats",
            "https://ela.st/es-deprecation-7-java-time",
            "Convert [date_time_field_Y] format dd-YYYY||MM-YYYY to java.time." + JODA_TIME_DEPRECATION_DETAILS_SUFFIX,
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, simpleIndex)
        );
        assertThat(issues, hasItem(expected));
    }

    public void testWarningsOnMixCustomAndDefinedPattern() throws IOException {
        String simpleMapping = "{\n"
            + "\"properties\" : {\n"
            + "   \"date_time_field_Y\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"strictWeekyearWeek||MM-YYYY\"\n"
            + "       }\n"
            + "   }"
            + "}";
        IndexMetadata simpleIndex = createV6Index(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Date fields use deprecated Joda time formats",
            "https://ela.st/es-deprecation-7-java-time",
            "Convert [date_time_field_Y] format strictWeekyearWeek||MM-YYYY to java.time." + JODA_TIME_DEPRECATION_DETAILS_SUFFIX,
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, simpleIndex)
        );
        assertThat(issues, hasItem(expected));
    }

    public void testJodaPatternDeprecations() throws IOException {
        String simpleMapping = "{\n"
            + "\"properties\" : {\n"
            + "   \"date_time_field_Y\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"MM-YYYY\"\n"
            + "       },\n"
            + "   \"date_time_field_C\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"CC\"\n"
            + "       },\n"
            + "   \"date_time_field_x\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"xx-MM\"\n"
            + "       },\n"
            + "   \"date_time_field_y\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"yy-MM\"\n"
            + "       },\n"
            + "   \"date_time_field_Z\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"HH:mmZ\"\n"
            + "       },\n"
            + "   \"date_time_field_z\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"HH:mmz\"\n"
            + "       }\n"
            + "   }"
            + "}";

        IndexMetadata simpleIndex = createV6Index(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Date fields use deprecated Joda time formats",
            "https://ela.st/es-deprecation-7-java-time",
            "Convert [date_time_field_Y] format MM-YYYY to java.time. Convert [date_time_field_C] format CC to java.time. Convert "
                + "[date_time_field_x] format xx-MM to java.time. Convert [date_time_field_y] format yy-MM to java.time. Convert "
                + "[date_time_field_Z] format HH:mmZ to java.time. Convert [date_time_field_z] format HH:mmz to java.time."
                + JODA_TIME_DEPRECATION_DETAILS_SUFFIX,
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, simpleIndex)
        );
        assertThat(issues, hasItem(expected));
    }

    public void testMultipleJodaPatternDeprecationInOneField() throws IOException {
        String simpleMapping = "{\n"
            + "\"properties\" : {\n"
            + "   \"date_time_field\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"Y-C-x-y\"\n"
            + "       }\n"
            + "   }"
            + "}";

        IndexMetadata simpleIndex = createV6Index(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Date fields use deprecated Joda time formats",
            "https://ela.st/es-deprecation-7-java-time",
            "Convert [date_time_field] format Y-C-x-y to java.time." + JODA_TIME_DEPRECATION_DETAILS_SUFFIX,
            false,
            null
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, simpleIndex)
        );
        assertThat(issues, hasItem(expected));
    }

    public IndexMetadata createV6Index(String simpleMapping) throws IOException {
        return IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(
                settings(VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, VersionUtils.getPreviousVersion(Version.V_7_0_0)))
            )
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 100))
            .putMapping("_doc", simpleMapping)
            .build();
    }

    static void addRandomFields(final int fieldLimit, XContentBuilder mappingBuilder) throws IOException {
        AtomicInteger fieldCount = new AtomicInteger(0);
        List<String> existingFieldNames = new ArrayList<>();
        while (fieldCount.get() < fieldLimit) {
            addRandomField(existingFieldNames, fieldLimit, mappingBuilder, fieldCount);
        }
    }

    private static void addRandomField(
        List<String> existingFieldNames,
        final int fieldLimit,
        XContentBuilder mappingBuilder,
        AtomicInteger fieldCount
    ) throws IOException {
        if (fieldCount.get() > fieldLimit) {
            return;
        }
        String newField = randomValueOtherThanMany(existingFieldNames::contains, () -> randomAlphaOfLengthBetween(2, 20));
        existingFieldNames.add(newField);
        mappingBuilder.startObject(newField);
        {
            if (rarely()) {
                mappingBuilder.startObject("properties");
                {
                    int subfields = randomIntBetween(1, 10);
                    while (existingFieldNames.size() < subfields && fieldCount.get() <= fieldLimit) {
                        addRandomField(existingFieldNames, fieldLimit, mappingBuilder, fieldCount);
                    }
                }
                mappingBuilder.endObject();
            } else {
                mappingBuilder.field("type", randomFrom("array", "range", "boolean", "date", "ip", "keyword", "text"));
                fieldCount.incrementAndGet();
            }
        }
        mappingBuilder.endObject();
    }

    public void testTranslogRetentionSettings() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
        settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Translog retention settings are deprecated",
                    "https://ela.st/es-deprecation-7-translog-settings",
                    "Remove the translog retention settings: \"index.translog.retention.size\" and \"index.translog.retention.age\". "
                        + "The translog has not been used in peer recoveries with soft-deletes enabled since 7.0 and these settings "
                        + "have no effect.",
                    false,
                    null
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
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        assertThat(issues, empty());
    }

    public void testFieldNamesEnabling() throws IOException {
        XContentBuilder xContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FieldNamesFieldMapper.NAME)
            .field("enabled", randomBoolean())
            .endObject()
            .endObject();
        String mapping = BytesReference.bytes(xContent).utf8ToString();

        IndexMetadata simpleIndex = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.CURRENT)))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("_doc", mapping)
            .build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, simpleIndex)
        );
        assertEquals(1, issues.size());

        DeprecationIssue issue = issues.get(0);
        assertEquals(DeprecationIssue.Level.WARNING, issue.getLevel());
        assertEquals("https://ela.st/es-deprecation-7-field_names-settings", issue.getUrl());
        assertEquals("Disabling the \"_field_names\" field in the index mappings is deprecated", issue.getMessage());
        assertEquals(
            "Remove the \"field_names\" mapping that configures the enabled setting. There's no longer a need to disable this "
                + "field to reduce index overhead if you have a lot of fields.",
            issue.getDetails()
        );
    }

    public void testIndexDataPathSetting() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(IndexMetadata.INDEX_DATA_PATH_SETTING.getKey(), createTempDir());
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-shared-path-settings";
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting [index.data_path] is deprecated",
                    expectedUrl,
                    "Remove the [index.data_path] setting. This setting has had no effect since 6.0.",
                    false,
                    null
                )
            )
        );
    }

    public void testSlowLogLevel() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), SlowLogLevel.DEBUG);
        settings.put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_LEVEL_SETTING.getKey(), SlowLogLevel.DEBUG);
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-slowlog-settings";
        assertThat(
            issues,
            containsInAnyOrder(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting [index.search.slowlog.level] is deprecated",
                    expectedUrl,
                    "Remove the [index.search.slowlog.level] setting. Use the [index.*.slowlog.threshold] settings to set the log levels.",
                    false,
                    null
                ),
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting [index.indexing.slowlog.level] is deprecated",
                    expectedUrl,
                    "Remove the [index.indexing.slowlog.level] setting. Use the [index.*.slowlog.threshold]"
                        + " settings to set the log levels.",
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
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting [index.store.type] to [simplefs] is deprecated",
                    "https://ela.st/es-deprecation-7-simplefs-store-type",
                    "Use [niofs] (the default) or one of the other FS types. This is an expert-only setting that might be removed in the "
                        + "future.",
                    false,
                    null
                )
            )
        );
    }

    public void testTierAllocationSettings() {
        String settingValue = DataTier.DATA_HOT;
        final Settings settings = settings(Version.CURRENT).put(INDEX_ROUTING_REQUIRE_SETTING.getKey(), DataTier.DATA_HOT)
            .put(INDEX_ROUTING_INCLUDE_SETTING.getKey(), DataTier.DATA_HOT)
            .put(INDEX_ROUTING_EXCLUDE_SETTING.getKey(), DataTier.DATA_HOT)
            .build();
        final DeprecationIssue expectedRequireIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", INDEX_ROUTING_REQUIRE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                INDEX_ROUTING_REQUIRE_SETTING.getKey(),
                settingValue
            ),
            false,
            null
        );
        final DeprecationIssue expectedIncludeIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", INDEX_ROUTING_INCLUDE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                INDEX_ROUTING_INCLUDE_SETTING.getKey(),
                settingValue
            ),
            false,
            null
        );
        final DeprecationIssue expectedExcludeIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", INDEX_ROUTING_EXCLUDE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                INDEX_ROUTING_EXCLUDE_SETTING.getKey()
            ),
            false,
            null
        );

        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        assertThat(IndexDeprecationChecks.checkIndexRoutingRequireSetting(indexMetadata), equalTo(expectedRequireIssue));
        assertThat(IndexDeprecationChecks.checkIndexRoutingIncludeSetting(indexMetadata), equalTo(expectedIncludeIssue));
        assertThat(IndexDeprecationChecks.checkIndexRoutingExcludeSetting(indexMetadata), equalTo(expectedExcludeIssue));

        final String warningTemplate = "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! "
            + "See the breaking changes documentation for the next major version.";
        final String[] expectedWarnings = {
            String.format(Locale.ROOT, warningTemplate, INDEX_ROUTING_REQUIRE_SETTING.getKey()),
            String.format(Locale.ROOT, warningTemplate, INDEX_ROUTING_INCLUDE_SETTING.getKey()),
            String.format(Locale.ROOT, warningTemplate, INDEX_ROUTING_EXCLUDE_SETTING.getKey()), };

        assertWarnings(expectedWarnings);
    }

    public void testCheckGeoShapeMappings() throws Exception {
        Map<String, Object> emptyMappingMap = Collections.emptyMap();
        MappingMetadata mappingMetadata = new MappingMetadata("", emptyMappingMap);
        Settings.Builder settings = settings(Version.CURRENT);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings)
            .putMapping(mappingMetadata)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        assertTrue(issues.isEmpty());

        Map<String, Object> okGeoMappingMap = Collections.singletonMap(
            "properties",
            Collections.singletonMap("location", Collections.singletonMap("type", "geo_shape"))
        );
        mappingMetadata = new MappingMetadata("", okGeoMappingMap);
        IndexMetadata indexMetadata2 = IndexMetadata.builder("test")
            .settings(settings)
            .putMapping(mappingMetadata)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata2));
        assertTrue(issues.isEmpty());

        Map<String, String> deprecatedPropertiesMap = Stream.of(
            new String[][] { { "type", "geo_shape" }, { "strategy", "recursive" }, { "points_only", "true" } }
        ).collect(Collectors.toMap(data -> data[0], data -> data[1]));
        Map<String, Object> deprecatedGeoMappingMap = Collections.singletonMap(
            "properties",
            Collections.singletonMap("location", deprecatedPropertiesMap)
        );
        mappingMetadata = new MappingMetadata("", deprecatedGeoMappingMap);
        IndexMetadata indexMetadata3 = IndexMetadata.builder("test")
            .settings(settings)
            .putMapping(mappingMetadata)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata3));
        assertEquals(1, issues.size());
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "[test] index uses deprecated geo_shape properties",
                    "https://ela.st/es-deprecation-7-geo-shape-mappings",
                    "The following geo_shape parameters must be removed from test: [[parameter [points_only] in field [location];"
                        + " parameter [strategy] in field [location]]]",
                    false,
                    null
                )
            )
        );

        Map<String, Object> nestedProperties = Stream.of(
            new Object[][] { { "type", "nested" }, { "properties", Collections.singletonMap("location", deprecatedPropertiesMap) }, }
        ).collect(Collectors.toMap(data -> (String) data[0], data -> data[1]));
        Map<String, Object> nestedDeprecatedGeoMappingMap = Collections.singletonMap(
            "properties",
            Collections.singletonMap("nested_field", nestedProperties)
        );
        mappingMetadata = new MappingMetadata("", nestedDeprecatedGeoMappingMap);
        IndexMetadata indexMetadata4 = IndexMetadata.builder("test")
            .settings(settings)
            .putMapping(mappingMetadata)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata4));
        assertEquals(1, issues.size());
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "[test] index uses deprecated geo_shape properties",
                    "https://ela.st/es-deprecation-7-geo-shape-mappings",
                    "The following geo_shape parameters must be removed from test: [[parameter [points_only] in field [location];"
                        + " parameter [strategy] in field [location]]]",
                    false,
                    null
                )
            )
        );
    }

    public void testAdjacencyMatrixSetting() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey(), 5);
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting [index.max_adjacency_matrix_filters] is deprecated",
                    "https://ela.st/es-deprecation-7-adjacency-matrix-filters-setting",
                    "Remove the [index.max_adjacency_matrix_filters] setting. Set [indices.query.bool.max_clause_count] to [5]. "
                        + "[index.max_adjacency_matrix_filters] will be ignored in 8.0.",
                    false,
                    null
                )
            )
        );

        String warningTemplate = "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! "
            + "See the breaking changes documentation for the next major version.";
        String[] expectedWarnings = {
            String.format(Locale.ROOT, warningTemplate, IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey()) };

        assertWarnings(expectedWarnings);
    }

    public void testFrozenIndex() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(FrozenEngine.INDEX_FROZEN.getKey(), true);
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Freezing indices is deprecated",
                    "https://ela.st/es-deprecation-7-frozen-indices",
                    "Index [test] is frozen. Frozen indices no longer offer any advantages. Instead, unfreeze the index, make it read-only,"
                        + " and move it to the cold or frozen tier.",
                    false,
                    null
                )
            )
        );
    }

    public void testEmptyDataTierPreference() {
        Settings.Builder settings = settings(Version.CURRENT);
        settings.put(DataTier.TIER_PREFERENCE_SETTING.getKey(), "  ");
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 100))
            .build();

        {
            List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
                INDEX_SETTINGS_CHECKS,
                c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
            );
            assertThat(issues, empty());
        }

        {
            ClusterState clusterState = clusterStateWithoutAllDataRoles();
            List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
                INDEX_SETTINGS_CHECKS,
                c -> c.apply(clusterState, indexMetadata)
            );
            assertThat(
                issues,
                contains(
                    new DeprecationIssue(
                        DeprecationIssue.Level.WARNING,
                        "Index [test] does not have a [index.routing.allocation.include._tier_preference] setting, "
                            + "in 8.0 this setting will be required for all indices and may not be empty or null.",
                        "https://ela.st/es-deprecation-7-empty-tier-preference",
                        "Update the settings for this index to specify an appropriate tier preference.",
                        false,
                        null
                    )
                )
            );
        }
    }

    private static ClusterState clusterStateWithoutAllDataRoles() {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> nodesList = org.elasticsearch.core.List.of(
            new DiscoveryNode(
                "name_0",
                "node_0",
                buildNewFakeTransportAddress(),
                org.elasticsearch.core.Map.of(),
                org.elasticsearch.core.Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE),
                Version.CURRENT
            )
        );
        for (DiscoveryNode node : nodesList) {
            discoBuilder = discoBuilder.add(node);
        }
        discoBuilder.localNodeId(randomFrom(nodesList).getId());
        discoBuilder.masterNodeId(randomFrom(nodesList).getId());

        return ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoBuilder.build()).build();
    }

    public void testForceMemoryTermDictionary() {
        Settings settings = Settings.builder()
            .put(Store.FORCE_RAM_TERM_DICT.getKey(), randomBoolean())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_7_0_0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [index.force_memory_term_dictionary] is deprecated",
            "https://ela.st/es-deprecation-7-force-memory-term-dictionary-setting",
            "Remove the [index.force_memory_term_dictionary] setting. This setting no longer has any effect.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            "[index.force_memory_term_dictionary] setting was deprecated in Elasticsearch and will be removed in a future "
                + "release! See the breaking changes documentation for the next major version."
        );
    }

    public void testMapperDynamicSetting() {
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), randomBoolean())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_7_0_0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(ClusterState.EMPTY_STATE, indexMetadata)
        );
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [index.mapper.dynamic] is deprecated",
            "https://ela.st/es-deprecation-7-mapper-dynamic-setting",
            "Remove the [index.mapper.dynamic] setting.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
    }
}
