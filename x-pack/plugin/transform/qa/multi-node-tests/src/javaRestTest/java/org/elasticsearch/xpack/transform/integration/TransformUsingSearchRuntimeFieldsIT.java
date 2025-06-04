/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static java.util.Map.entry;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class TransformUsingSearchRuntimeFieldsIT extends TransformRestTestCase {

    private static final String REVIEWS_INDEX_NAME = "basic-crud-reviews";
    private static final int NUM_USERS = 28;

    private static Map<String, Object> createRuntimeMappings() {
        return Map.ofEntries(
            entry(
                "user-upper",
                Map.of(
                    "type",
                    "keyword",
                    "script",
                    Map.of("source", "if (params._source.user_id != null) {emit(params._source.user_id.toUpperCase())}")
                )
            ),
            entry("stars", Map.of("type", "long")),
            entry(
                "stars-x2",
                Map.of("type", "long", "script", Map.of("source", "if (params._source.stars != null) {emit(2 * params._source.stars)}"))
            ),
            entry(
                "timestamp-5m",
                Map.of(
                    "type",
                    "date",
                    "script",
                    Map.of("source", "emit(doc['timestamp'].value.toInstant().minus(5, ChronoUnit.MINUTES).toEpochMilli())")
                )
            )
        );
    }

    @Before
    public void createReviewsIndex() throws Exception {
        createReviewsIndex(REVIEWS_INDEX_NAME, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);
    }

    @After
    public void cleanTransforms() throws Exception {
        cleanUp();
    }

    @SuppressWarnings("unchecked")
    public void testPivotTransform() throws Exception {
        String destIndexName = "reviews-by-user-pivot";
        String transformId = "transform-with-st-rt-fields-pivot";
        Map<String, Object> runtimeMappings = createRuntimeMappings();

        Map<String, SingleGroupSource> groups = singletonMap("by-user", new TermsGroupSource("user-upper", null, false));
        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("review_score_max").field("stars"))
            .addAggregator(AggregationBuilders.avg("review_score_rt_avg").field("stars-x2"))
            .addAggregator(AggregationBuilders.max("review_score_rt_max").field("stars-x2"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"))
            .addAggregator(AggregationBuilders.max("timestamp_rt").field("timestamp-5m"));
        TransformConfig config = createTransformConfigBuilder(transformId, destIndexName, QueryConfig.matchAll(), "dummy").setSource(
            new SourceConfig(new String[] { REVIEWS_INDEX_NAME }, QueryConfig.matchAll(), runtimeMappings)
        ).setPivotConfig(createPivotConfig(groups, aggs)).build();

        var previewResponse = previewTransform(Strings.toString(config), RequestOptions.DEFAULT);
        // Verify preview mappings
        Map<String, Object> expectedMappingProperties = Map.ofEntries(
            entry("by-user", Map.of("type", "keyword")),
            entry("review_score", Map.of("type", "double")),
            entry("review_score_max", Map.of("type", "long")),
            entry("review_score_rt_avg", Map.of("type", "double")),
            entry("review_score_rt_max", Map.of("type", "long")),
            entry("timestamp", Map.of("type", "date")),
            entry("timestamp_rt", Map.of("type", "date"))
        );
        var generatedMappings = (Map<String, Object>) XContentMapValues.extractValue("generated_dest_index.mappings", previewResponse);
        assertThat(generatedMappings, allOf(hasKey("_meta"), hasEntry("properties", expectedMappingProperties)));
        // Verify preview contents
        var previewDocs = (List<Map<String, Object>>) XContentMapValues.extractValue("preview", previewResponse);
        assertThat(previewDocs, hasSize(NUM_USERS));
        previewDocs.forEach(doc -> {
            assertThat((String) doc.get("by-user"), isUpperCase());
            assertThat(doc.get("review_score_rt_avg"), is(equalTo(2 * (double) doc.get("review_score"))));
            assertThat(doc.get("review_score_rt_max"), is(equalTo(2 * (int) doc.get("review_score_max"))));
            assertThat(
                Instant.parse((String) doc.get("timestamp_rt")),
                is(equalTo(Instant.parse((String) doc.get("timestamp")).minus(5, ChronoUnit.MINUTES)))
            );
        });

        putTransform(config.getId(), Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        waitUntilCheckpoint(config.getId(), 1L);

        stopTransform(config.getId());
        assertBusy(() -> { assertEquals("stopped", getTransformState(config.getId())); });

        refreshIndex(destIndexName);
        // Verify destination index mappings
        var mappings = (Map<String, Object>) XContentMapValues.extractValue(
            destIndexName + ".mappings",
            getIndexMapping(destIndexName, RequestOptions.DEFAULT)
        );
        assertThat(mappings, allOf(hasKey("_meta"), hasKey("properties")));
        // Verify destination index contents
        var searchResponse = matchAllSearch(destIndexName, 1000, RequestOptions.DEFAULT);
        assertThat((Integer) XContentMapValues.extractValue("hits.total.value", searchResponse), is(equalTo(NUM_USERS)));

        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", searchResponse);
        var docs = hits.stream().map(h -> (Map<String, Object>) h.get("_source")).collect(Collectors.toList());
        assertThat(docs, is(equalTo(previewDocs)));
    }

    public void testPivotTransform_BadRuntimeFieldScript() throws Exception {
        String destIndexName = "reviews-by-user-pivot";
        String transformId = "transform-with-st-rt-fields-pivot";
        Map<String, Object> runtimeMappings = Map.of(
            "user-upper",
            Map.of(
                "type",
                "keyword",
                // Method name used in the script is misspelled, i.e.: "toUperCase" instead of "toUpperCase"
                "script",
                Map.of("source", "if (params._source.user_id != null) {emit(params._source.user_id.toUperCase())}")
            )
        );

        Map<String, SingleGroupSource> groups = singletonMap("by-user", new TermsGroupSource("user-upper", null, false));
        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.avg("review_score_rt").field("stars-x2"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"))
            .addAggregator(AggregationBuilders.max("timestamp_rt").field("timestamp-5m"));
        TransformConfig config = createTransformConfigBuilder(transformId, destIndexName, QueryConfig.matchAll(), "dummy").setSource(
            new SourceConfig(new String[] { REVIEWS_INDEX_NAME }, QueryConfig.matchAll(), runtimeMappings)
        ).setPivotConfig(createPivotConfig(groups, aggs)).build();

        Exception e = expectThrows(Exception.class, () -> previewTransform(Strings.toString(config), RequestOptions.DEFAULT));
        assertThat(
            ExceptionsHelper.stackTrace(e),
            allOf(containsString("script_exception"), containsString("dynamic method [java.lang.String, toUperCase/0] not found"))
        );

        e = expectThrows(Exception.class, () -> putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT));
        assertThat(
            ExceptionsHelper.stackTrace(e),
            allOf(containsString("script_exception"), containsString("dynamic method [java.lang.String, toUperCase/0] not found"))
        );
    }

    @SuppressWarnings("unchecked")
    public void testLatestTransform() throws Exception {
        String destIndexName = "reviews-by-user-latest";
        String transformId = "transform-with-st-rt-fields-latest";
        Map<String, Object> runtimeMappings = createRuntimeMappings();

        SourceConfig sourceConfig = new SourceConfig(new String[] { REVIEWS_INDEX_NAME }, QueryConfig.matchAll(), runtimeMappings);
        TransformConfig configWithOrdinaryFields = createTransformConfigBuilder(transformId, destIndexName, QueryConfig.matchAll(), "dummy")
            .setSource(sourceConfig)
            .setLatestConfig(new LatestConfig(List.of("user_id"), "timestamp"))
            .build();

        var previewWithOrdinaryFields = previewTransform(Strings.toString(configWithOrdinaryFields), RequestOptions.DEFAULT);
        // Verify preview mappings
        var generatedMappings = (Map<String, Object>) XContentMapValues.extractValue(
            "generated_dest_index.mappings",
            previewWithOrdinaryFields
        );
        assertThat(generatedMappings, allOf(hasKey("_meta"), hasKey("properties")));
        // Verify preview contents
        var docsWithOrdinaryFields = (List<Map<String, Object>>) previewWithOrdinaryFields.get("preview");
        assertThat("Got preview: " + previewWithOrdinaryFields, docsWithOrdinaryFields, hasSize(NUM_USERS));
        docsWithOrdinaryFields.forEach(doc -> {
            assertThat(doc, hasKey("user_id"));
            assertThat(doc, not(hasKey("user-upper")));
        });

        TransformConfig configWithRuntimeFields = createTransformConfigBuilder(transformId, destIndexName, QueryConfig.matchAll(), "dummy")
            .setSource(sourceConfig)
            .setLatestConfig(new LatestConfig(List.of("user-upper"), "timestamp-5m"))
            .build();

        var previewWithRuntimeFields = previewTransform(Strings.toString(configWithRuntimeFields), RequestOptions.DEFAULT);
        var docsWithRuntimeFields = (List<Map<String, Object>>) previewWithRuntimeFields.get("preview");
        assertThat(docsWithRuntimeFields, is(equalTo(docsWithOrdinaryFields)));

        putTransform(configWithRuntimeFields.getId(), Strings.toString(configWithRuntimeFields), RequestOptions.DEFAULT);
        startTransform(configWithRuntimeFields.getId(), RequestOptions.DEFAULT);

        waitUntilCheckpoint(configWithRuntimeFields.getId(), 1L);

        stopTransform(configWithRuntimeFields.getId());
        assertBusy(() -> { assertEquals("stopped", getTransformState(configWithRuntimeFields.getId())); });

        refreshIndex(destIndexName);
        // Verify destination index mappings
        var destIndexMapping = getIndexMapping(destIndexName, RequestOptions.DEFAULT);

        assertThat(
            (Map<String, Object>) XContentMapValues.extractValue(destIndexName + ".mappings", destIndexMapping),
            allOf(hasKey("_meta"), hasKey("properties"))
        );

        // Verify destination index contents
        Request searchRequest = new Request("GET", destIndexName + "/_search");
        searchRequest.addParameter("size", "1000");
        Response searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);
        var searchMap = entityAsMap(searchResponse);
        assertThat((Integer) XContentMapValues.extractValue("hits.total.value", searchMap), is(equalTo(NUM_USERS)));
        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", searchMap);
        var searchDocs = hits.stream().map(h -> (Map<String, Object>) h.get("_source")).collect(Collectors.toList());
        assertThat(searchDocs, is(equalTo(docsWithOrdinaryFields)));
    }

    public void testLatestTransform_BadRuntimeFieldScript() throws Exception {
        String destIndexName = "reviews-by-user-latest";
        String transformId = "transform-with-st-rt-fields-latest";
        Map<String, Object> runtimeMappings = Map.of(
            "user-upper",
            Map.of(
                "type",
                "keyword",
                // Method name used in the script is misspelled, i.e.: "toUperCase" instead of "toUpperCase"
                "script",
                Map.of("source", "if (params._source.user_id != null) {emit(params._source.user_id.toUperCase())}")
            )
        );

        SourceConfig sourceConfig = new SourceConfig(new String[] { REVIEWS_INDEX_NAME }, QueryConfig.matchAll(), runtimeMappings);
        TransformConfig configWithRuntimeFields = createTransformConfigBuilder(transformId, destIndexName, QueryConfig.matchAll(), "dummy")
            .setSource(sourceConfig)
            .setLatestConfig(new LatestConfig(List.of("user-upper"), "timestamp"))
            .build();

        var stringConfig = Strings.toString(configWithRuntimeFields);
        Exception e = expectThrows(Exception.class, () -> previewTransform(stringConfig, RequestOptions.DEFAULT));
        assertThat(
            ExceptionsHelper.stackTrace(e),
            allOf(containsString("script_exception"), containsString("dynamic method [java.lang.String, toUperCase/0] not found"))
        );

        e = expectThrows(Exception.class, () -> putTransform(transformId, stringConfig, RequestOptions.DEFAULT));
        assertThat(
            ExceptionsHelper.stackTrace(e),
            allOf(containsString("script_exception"), containsString("dynamic method [java.lang.String, toUperCase/0] not found"))
        );
    }

    private static IsUpperCaseMatcher isUpperCase() {
        return new IsUpperCaseMatcher();
    }

    private static class IsUpperCaseMatcher extends TypeSafeMatcher<String> {

        @Override
        protected boolean matchesSafely(String item) {
            return item.chars().noneMatch(Character::isLowerCase);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("an upper-case string");
        }
    }
}
