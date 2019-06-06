/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.JodaDeprecationPatterns;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.DEFAULT_MAPPING;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class IndexDeprecationChecksTests extends ESTestCase {

    public void testOldIndicesCheck() {
        Version createdWith = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0,
            VersionUtils.getPreviousVersion(Version.V_6_0_0));
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(settings(createdWith))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Index created before 6.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_indices_created_before_7_0",
            "This index was created using version: " + createdWith);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testOldTasksIndexCheck() {
        Version createdWith = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0,
            VersionUtils.getPreviousVersion(Version.V_6_0_0));
        IndexMetaData indexMetaData = IndexMetaData.builder(".tasks")
            .settings(settings(createdWith))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            ".tasks index must be re-created",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_indices_created_before_7_0",
            "The .tasks index was created before version 6.0 and cannot be opened in 7.0. " +
                "You must delete this index and allow it to be re-created by Elasticsearch. If you wish to preserve task history, " +
                "reindex this index to a new index before deleting it.");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testUnupgradedWatcherIndexCheck() {
        Version createdWith = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0,
            VersionUtils.getPreviousVersion(Version.V_6_0_0));
        IndexMetaData indexMetaData = IndexMetaData.builder(".watches")
            .settings(settings(createdWith))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            ".watches was not properly upgraded before upgrading to Elasticsearch 6",
            "https://www.elastic.co/guide/en/elasticsearch/reference/current/migration-api-upgrade.html",
            "The .watches index was created before version 6.0, and was not properly upgraded in 5.6. " +
                "Please upgrade this index using the Migration Upgrade API.");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testMultipleTypesCheckWithDefaultMapping() throws IOException {
        String mappingName1 = randomAlphaOfLengthBetween(2, 5);
        String mappingJson1 = "{\n" +
            "  \"properties\": {\n" +
            "    \"field_a\": {\n" +
            "      \"type\": \"text\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String defaultMappingJson = "{\n" +
            "  \"properties\": {\n" +
            "    \"field_b\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

        Version createdWith = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0,
            VersionUtils.getPreviousVersion(Version.V_6_0_0));
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .putMapping(mappingName1, mappingJson1)
            .putMapping(DEFAULT_MAPPING, defaultMappingJson)
            .settings(settings(createdWith))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Index created before 6.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_indices_created_before_7_0",
            "This index was created using version: " + createdWith);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testMultipleTypesCheck() throws IOException {
        String mappingName1 = randomAlphaOfLengthBetween(2, 5);
        String mappingJson1 = "{\n" +
            "  \"properties\": {\n" +
            "    \"field_a\": {\n" +
            "      \"type\": \"text\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String mappingName2 = randomAlphaOfLengthBetween(6, 10);
        String mappingJson2 = "{\n" +
            "  \"properties\": {\n" +
            "    \"field_b\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

        Version createdWith = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0,
            VersionUtils.getPreviousVersion(Version.V_6_0_0));
        IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder("test")
            .putMapping(mappingName1, mappingJson1)
            .putMapping(mappingName2, mappingJson2)
            .settings(settings(createdWith))
            .numberOfShards(1)
            .numberOfReplicas(0);

        if (randomBoolean()) {
            String defaultMappingJson = "{\n" +
                "  \"properties\": {\n" +
                "    \"field_c\": {\n" +
                "      \"type\": \"keyword\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
            indexMetaDataBuilder.putMapping(DEFAULT_MAPPING, defaultMappingJson);
        }

        IndexMetaData indexMetaData = indexMetaDataBuilder.build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(1, issues.size());
        DeprecationIssue issue = issues.get(0);
        assertEquals("Index has more than one mapping type", issue.getMessage());
        assertEquals(DeprecationIssue.Level.CRITICAL, issue.getLevel());
        assertEquals(
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/removal-of-types.html" +
                "#_migrating_multi_type_indices_to_single_type",
            issue.getUrl());
        assertThat(issue.getDetails(), allOf(
            containsString("This index has more than one mapping type, which is not supported in 7.0. " +
                "This index must be reindexed into one or more single-type indices. Mapping types in use: ["),
            containsString(mappingName1),
            containsString(mappingName2)));
    }

    public void testDelimitedPayloadFilterCheck() {
        Settings settings = settings(
            VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, VersionUtils.getPreviousVersion(Version.CURRENT)))
            .put("index.analysis.filter.my_delimited_payload_filter.type", "delimited_payload_filter")
            .put("index.analysis.filter.my_delimited_payload_filter.delimiter", "^")
            .put("index.analysis.filter.my_delimited_payload_filter.encoding", "identity").build();
        IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING, "Use of 'delimited_payload_filter'.",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_literal_delimited_payload_filter_literal_renaming",
            "[The filter [my_delimited_payload_filter] is of deprecated 'delimited_payload_filter' type. "
                + "The filter type should be changed to 'delimited_payload'.]");
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testIndexNameCheck(){
        final String badIndexName = randomAlphaOfLengthBetween(0, 10) + ":" + randomAlphaOfLengthBetween(0, 10);
        final IndexMetaData badIndex = IndexMetaData.builder(badIndexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1,100))
            .numberOfReplicas(randomIntBetween(1,15))
            .build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING, "Index name cannot contain ':'",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_literal_literal_is_no_longer_allowed_in_index_name",
            "This index is named [" + badIndexName + "], which contains the illegal character ':'.");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(badIndex));
        assertEquals(singletonList(expected), issues);

        final String goodIndexName = randomAlphaOfLengthBetween(1,30);
        final IndexMetaData goodIndex = IndexMetaData.builder(goodIndexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1,100))
            .numberOfReplicas(randomIntBetween(1,15))
            .build();
        List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(goodIndex));
        assertTrue(noIssues.isEmpty());
    }

    public void testPercolatorUnmappedFieldsAsStringCheck() {
        boolean settingValue = randomBoolean();
        Settings settings = settings(
            VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, VersionUtils.getPreviousVersion(Version.CURRENT)))
            .put("index.percolator.map_unmapped_fields_as_text", settingValue).build();
        final IndexMetaData badIndex = IndexMetaData.builder(randomAlphaOfLengthBetween(1,30).toLowerCase(Locale.ROOT))
            .settings(settings)
            .numberOfShards(randomIntBetween(1,100))
            .numberOfReplicas(randomIntBetween(1,15))
            .build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Setting index.percolator.map_unmapped_fields_as_text has been renamed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_percolator",
            "The index setting [index.percolator.map_unmapped_fields_as_text] currently set to [" + settingValue +
                "] been removed in favor of [index.percolator.map_unmapped_fields_as_text].");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(badIndex));
        assertEquals(singletonList(expected), issues);

        final IndexMetaData goodIndex = IndexMetaData.builder(randomAlphaOfLengthBetween(1,30).toLowerCase(Locale.ROOT))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1,100))
            .numberOfReplicas(randomIntBetween(1,15))
            .build();
        List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(goodIndex));
        assertTrue(noIssues.isEmpty());
    }

    public void testClassicSimilarityMappingCheck() throws IOException {
        String mappingJson = "{\n" +
            "  \"properties\": {\n" +
            "    \"default_field\": {\n" +
            "      \"type\": \"text\"\n" +
            "    },\n" +
            "    \"classic_sim_field\": {\n" +
            "      \"type\": \"text\",\n" +
            "      \"similarity\": \"classic\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        IndexMetaData index = createSimpleIndex(mappingJson);
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Classic similarity has been removed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_the_literal_classic_literal_similarity_has_been_removed",
            "Fields which use classic similarity: [[type: _doc, field: classic_sim_field]]");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(index));
        assertEquals(singletonList(expected), issues);
    }

    public void testClassicSimilaritySettingsCheck() {
        IndexMetaData index = IndexMetaData.builder(randomAlphaOfLengthBetween(5, 10))
            .settings(settings(
                VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, VersionUtils.getPreviousVersion(Version.CURRENT)))
                .put("index.similarity.my_classic_similarity.type", "classic")
                .put("index.similarity.my_okay_similarity.type", "BM25"))
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 100))
            .build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Classic similarity has been removed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_the_literal_classic_literal_similarity_has_been_removed",
            "Custom similarities defined using classic similarity: [my_classic_similarity]");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(index));
        assertEquals(singletonList(expected), issues);
    }

    public void testNodeLeftDelayedTimeCheck() {
        String negativeTimeValue = "-" + randomPositiveTimeValue();
        String indexName = randomAlphaOfLengthBetween(0, 10);
        String setting = UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey();

        final IndexMetaData badIndex = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT).put(setting, negativeTimeValue))
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 15))
            .build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Negative values for " + setting + " are deprecated and should be set to 0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_literal_index_unassigned_node_left_delayed_timeout_literal_may_no_longer_be_negative",
            "The index [" + indexName + "] has [" + setting + "] set to [" + negativeTimeValue +
                "], but negative values are not allowed");

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(badIndex));
        assertEquals(singletonList(expected), issues);

        final IndexMetaData goodIndex = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 15))
            .build();
        List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(goodIndex));
        assertTrue(noIssues.isEmpty());
    }

    public void testShardOnStartupCheck() {
        String indexName = randomAlphaOfLengthBetween(0, 10);
        String setting = IndexSettings.INDEX_CHECK_ON_STARTUP.getKey();
        final IndexMetaData badIndex = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT).put(setting, "fix"))
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 15))
            .build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "The value [fix] for setting [" + setting + "] is no longer valid",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_literal_fix_literal_value_for_literal_index_shard_check_on_startup_literal_is_removed",
            "The index [" + indexName + "] has the setting [" + setting + "] set to value [fix]" +
                ", but [fix] is no longer a valid value. Valid values are true, false, and checksum");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(badIndex));
        assertEquals(singletonList(expected), issues);
        final IndexMetaData goodIndex = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 100))
            .numberOfReplicas(randomIntBetween(1, 15))
            .build();
        List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(goodIndex));
        assertTrue(noIssues.isEmpty());
    }

    public void testTooManyFieldsCheck() throws IOException {
        String simpleMapping = "{\n" +
            "  \"properties\": {\n" +
            "    \"some_field\": {\n" +
            "      \"type\": \"text\"\n" +
            "    },\n" +
            "    \"other_field\": {\n" +
            "      \"type\": \"text\",\n" +
            "      \"properties\": {\n" +
            "        \"raw\": {\"type\": \"keyword\"}\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        IndexMetaData simpleIndex = createSimpleIndex(simpleMapping);
        List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
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

        IndexMetaData tooManyFieldsIndex = createSimpleIndex(Strings.toString(mappingBuilder));
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Number of fields exceeds automatic field expansion limit",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_limiting_the_number_of_auto_expanded_fields",
            "This index has [" + fieldCount + "] fields, which exceeds the automatic field expansion limit of 1024 " +
                "and does not have [" + IndexSettings.DEFAULT_FIELD_SETTING_KEY + "] set, which may cause queries which use " +
                "automatic field expansion, such as query_string, simple_query_string, and multi_match to fail if fields are not " +
                "explicitly specified in the query.");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(tooManyFieldsIndex));
        assertEquals(singletonList(expected), issues);

        // Check that it's okay to  have too many fields as long as `index.query.default_field` is set
        IndexMetaData tooManyFieldsOk = IndexMetaData.builder(randomAlphaOfLengthBetween(5,10))
            .settings(settings(
                VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, VersionUtils.getPreviousVersion(Version.CURRENT)))
                .put(IndexSettings.DEFAULT_FIELD_SETTING_KEY, randomAlphaOfLength(5)))
            .numberOfShards(randomIntBetween(1,100))
            .numberOfReplicas(randomIntBetween(1, 100))
            .putMapping("_doc", Strings.toString(mappingBuilder))
            .build();
        List<DeprecationIssue> withDefaultFieldIssues =
            DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(tooManyFieldsOk));
        assertEquals(0, withDefaultFieldIssues.size());
    }

    public void testDefinedPatternsDoNotWarn() throws IOException {
        String simpleMapping = "{\n" +
            "\"properties\" : {\n" +
            "   \"date_time_field_Y\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"strictWeekyearWeek\"\n" +
            "       }\n" +
            "   }" +
            "}";
        IndexMetaData simpleIndex = createSimpleIndex(simpleMapping);

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
        assertThat(issues, is(emptyList()));
    }

    public void testMigratedPatterns() throws IOException {
        String simpleMapping = "{\n" +
            "\"properties\" : {\n" +
            "   \"date_time_field_Y\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"8MM-YYYY\"\n" +
            "       }\n" +
            "   }" +
            "}";
        IndexMetaData simpleIndex = createSimpleIndex(simpleMapping);

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
        assertThat(issues, is(emptyList()));
    }

    public void testMultipleWarningsOnCombinedPattern() throws IOException {
        String simpleMapping = "{\n" +
            "\"properties\" : {\n" +
            "   \"date_time_field_Y\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"dd-CC||MM-YYYY\"\n" +
            "       }\n" +
            "   }" +
            "}";
        IndexMetaData simpleIndex = createSimpleIndex(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Date field format uses patterns which may change meaning in 7.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#breaking_70_java_time_changes",
            "This index has date fields with deprecated formats: ["+
                "[type: _doc, field: date_time_field_Y, format: dd-CC||MM-YYYY, " +
                "suggestion: 'C' century of era is no longer supported." +
                "; "+
                "'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.]"+
                "]. "+ JodaDeprecationPatterns.USE_PREFIX_8_WARNING);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
        assertEquals(singletonList(expected), issues);
    }

    public void testDuplicateWarningsOnCombinedPattern() throws IOException {
        String simpleMapping = "{\n" +
            "\"properties\" : {\n" +
            "   \"date_time_field_Y\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"dd-YYYY||MM-YYYY\"\n" +
            "       }\n" +
            "   }" +
            "}";
        IndexMetaData simpleIndex = createSimpleIndex(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Date field format uses patterns which may change meaning in 7.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#breaking_70_java_time_changes",
            "This index has date fields with deprecated formats: ["+
                "[type: _doc, field: date_time_field_Y, format: dd-YYYY||MM-YYYY, " +
                "suggestion: 'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.]"+
                "]. "+ JodaDeprecationPatterns.USE_PREFIX_8_WARNING);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
        assertEquals(singletonList(expected), issues);
    }

    public void testWarningsOnMixCustomAndDefinedPattern() throws IOException {
        String simpleMapping = "{\n" +
            "\"properties\" : {\n" +
            "   \"date_time_field_Y\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"strictWeekyearWeek||MM-YYYY\"\n" +
            "       }\n" +
            "   }" +
            "}";
        IndexMetaData simpleIndex = createSimpleIndex(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Date field format uses patterns which may change meaning in 7.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#breaking_70_java_time_changes",
            "This index has date fields with deprecated formats: ["+
                "[type: _doc, field: date_time_field_Y, format: strictWeekyearWeek||MM-YYYY, " +
                "suggestion: 'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.]"+
                "]. "+ JodaDeprecationPatterns.USE_PREFIX_8_WARNING);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
        assertEquals(singletonList(expected), issues);
    }

    public void testJodaPatternDeprecations() throws IOException {
        String simpleMapping = "{\n" +
            "\"properties\" : {\n" +
            "   \"date_time_field_Y\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"MM-YYYY\"\n" +
            "       },\n" +
            "   \"date_time_field_C\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"CC\"\n" +
            "       },\n" +
            "   \"date_time_field_x\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"xx-MM\"\n" +
            "       },\n" +
            "   \"date_time_field_y\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"yy-MM\"\n" +
            "       },\n" +
            "   \"date_time_field_Z\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"HH:mmZ\"\n" +
            "       },\n" +
            "   \"date_time_field_z\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"HH:mmz\"\n" +
            "       }\n" +
            "   }" +
            "}";

        IndexMetaData simpleIndex = createSimpleIndex(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Date field format uses patterns which may change meaning in 7.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#breaking_70_java_time_changes",
            "This index has date fields with deprecated formats: ["+
                 "[type: _doc, field: date_time_field_Y, format: MM-YYYY, " +
                "suggestion: 'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.], "+
                 "[type: _doc, field: date_time_field_C, format: CC, " +
                "suggestion: 'C' century of era is no longer supported.], "+
                 "[type: _doc, field: date_time_field_x, format: xx-MM, " +
                "suggestion: 'x' weak-year should be replaced with 'Y'. Use 'x' for zone-offset.], "+
                 "[type: _doc, field: date_time_field_y, format: yy-MM, " +
                "suggestion: 'y' year should be replaced with 'u'. Use 'y' for year-of-era.], "+
                 "[type: _doc, field: date_time_field_Z, format: HH:mmZ, " +
                "suggestion: 'Z' time zone offset/id fails when parsing 'Z' for Zulu timezone. Consider using 'X'.], "+
                 "[type: _doc, field: date_time_field_z, format: HH:mmz, " +
                "suggestion: 'z' time zone text. Will print 'Z' for Zulu given UTC timezone." +
                "]"+
                "]. "+ JodaDeprecationPatterns.USE_PREFIX_8_WARNING);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
        assertEquals(singletonList(expected), issues);
    }

    public void testMultipleJodaPatternDeprecationInOneField() throws IOException {
        String simpleMapping = "{\n" +
            "\"properties\" : {\n" +
            "   \"date_time_field\" : {\n" +
            "       \"type\" : \"date\",\n" +
            "       \"format\" : \"Y-C-x-y\"\n" +
            "       }\n" +
            "   }" +
            "}";

        IndexMetaData simpleIndex = createSimpleIndex(simpleMapping);

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Date field format uses patterns which may change meaning in 7.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#breaking_70_java_time_changes",
            "This index has date fields with deprecated formats: ["+
                "[type: _doc, field: date_time_field, format: Y-C-x-y, " +
                "suggestion: 'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.; " +
                "'y' year should be replaced with 'u'. Use 'y' for year-of-era.; " +
                "'C' century of era is no longer supported.; " +
                "'x' weak-year should be replaced with 'Y'. Use 'x' for zone-offset." +
                "]"+
                "]. "+ JodaDeprecationPatterns.USE_PREFIX_8_WARNING);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(simpleIndex));
        assertEquals(singletonList(expected), issues);
    }

    public IndexMetaData createSimpleIndex(String simpleMapping) throws IOException {
        return IndexMetaData.builder(randomAlphaOfLengthBetween(5, 10))
                            .settings(settings(
                                VersionUtils.randomVersionBetween(random(), Version.V_6_0_0,
                                    VersionUtils.getPreviousVersion(Version.CURRENT))))
                            .numberOfShards(randomIntBetween(1, 100))
                            .numberOfReplicas(randomIntBetween(1, 100))
                            .putMapping("_doc", simpleMapping)
                            .build();
    }

    static void addRandomFields(final int fieldLimit,
                                        XContentBuilder mappingBuilder) throws IOException {
        AtomicInteger fieldCount = new AtomicInteger(0);
        List<String> existingFieldNames = new ArrayList<>();
        while (fieldCount.get() < fieldLimit) {
            addRandomField(existingFieldNames, fieldLimit, mappingBuilder, fieldCount);
        }
    }

    private static void addRandomField(List<String> existingFieldNames, final int fieldLimit,
                                       XContentBuilder mappingBuilder, AtomicInteger fieldCount) throws IOException {
        if (fieldCount.get() > fieldLimit) {
            return;
        }
        String newField = randomValueOtherThanMany(existingFieldNames::contains, () -> randomAlphaOfLengthBetween(2,20));
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
}
