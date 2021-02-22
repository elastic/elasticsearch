/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.transform.PreviewTransformResponse;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.client.transform.transforms.latest.LatestConfig;
import org.elasticsearch.client.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class TransformUsingSearchRuntimeFieldsIT extends TransformIntegTestCase {

    private static final String REVIEWS_INDEX_NAME = "basic-crud-reviews";
    private static final int NUM_USERS = 28;

    private static final Integer getUserIdForRow(int row) {
        return row % NUM_USERS;
    }

    private static final String getDateStringForRow(int row) {
        int day = (11 + (row / 100)) % 28;
        int hour = 10 + (row % 13);
        int min = 10 + (row % 49);
        int sec = 10 + (row % 49);
        return "2017-01-" + (day < 10 ? "0" + day : day) + "T" + hour + ":" + min + ":" + sec + "Z";
    }

    private static Map<String, Object> createRuntimeMappings() {
        return new HashMap<>() {{
            put("user-upper", new HashMap<>() {{
                put("type", "keyword");
                put("script", singletonMap("source", "if (params._source.user_id != null) {emit(params._source.user_id.toUpperCase())}"));
            }});
            put("stars", new HashMap<>() {{
                put("type", "long");
            }});
            put("stars-x2", new HashMap<>() {{
                put("type", "long");
                put("script", singletonMap("source", "if (params._source.stars != null) {emit(2 * params._source.stars)}"));
            }});
            put("timestamp-5m", new HashMap<>() {{
                put("type", "date");
                put("script", singletonMap(
                    "source", "emit(doc['timestamp'].value.toInstant().minus(5, ChronoUnit.MINUTES).toEpochMilli())"));
            }});
        }};
    }

    @Before
    public void createReviewsIndex() throws Exception {
        createReviewsIndex(
            REVIEWS_INDEX_NAME,
            100,
            NUM_USERS,
            TransformUsingSearchRuntimeFieldsIT::getUserIdForRow,
            TransformUsingSearchRuntimeFieldsIT::getDateStringForRow);
    }

    @After
    public void cleanTransforms() throws IOException {
        cleanUp();
    }

    public void testPivotTransform() throws Exception {
        String destIndexName = "reviews-by-user-pivot";
        String transformId = "transform-with-st-rt-fields-pivot";
        Map<String, Object> runtimeMappings = createRuntimeMappings();

        Map<String, SingleGroupSource> groups = singletonMap("by-user", TermsGroupSource.builder().setField("user-upper").build());
        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("review_score_max").field("stars"))
            .addAggregator(AggregationBuilders.avg("review_score_rt_avg").field("stars-x2"))
            .addAggregator(AggregationBuilders.max("review_score_rt_max").field("stars-x2"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"))
            .addAggregator(AggregationBuilders.max("timestamp_rt").field("timestamp-5m"));
        TransformConfig config =
            createTransformConfigBuilder(transformId, destIndexName, QueryBuilders.matchAllQuery(), "dummy")
                .setSource(SourceConfig.builder()
                    .setIndex(REVIEWS_INDEX_NAME)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setRuntimeMappings(runtimeMappings)
                    .build())
                .setPivotConfig(createPivotConfig(groups, aggs))
                .build();

        PreviewTransformResponse previewResponse = previewTransform(config, RequestOptions.DEFAULT);
        // Verify preview mappings
        Map<String, Object> expectedMappingProperties =
            new HashMap<>() {{
                put("by-user", singletonMap("type", "keyword"));
                put("review_score", singletonMap("type", "double"));
                put("review_score_max", singletonMap("type", "long"));
                put("review_score_rt_avg", singletonMap("type", "double"));
                put("review_score_rt_max", singletonMap("type", "long"));
                put("timestamp", singletonMap("type", "date"));
                put("timestamp_rt", singletonMap("type", "date"));
            }};
        assertThat(previewResponse.getMappings(), allOf(hasKey("_meta"), hasEntry("properties", expectedMappingProperties)));
        // Verify preview contents
        assertThat(previewResponse.getDocs(), hasSize(NUM_USERS));
        previewResponse.getDocs().forEach(
            doc -> {
                assertThat((String) doc.get("by-user"), isUpperCase());
                assertThat(doc.get("review_score_rt_avg"), is(equalTo(2 * (double) doc.get("review_score"))));
                assertThat(doc.get("review_score_rt_max"), is(equalTo(2 * (int) doc.get("review_score_max"))));
                assertThat(
                    Instant.parse((String) doc.get("timestamp_rt")),
                    is(equalTo(Instant.parse((String) doc.get("timestamp")).minus(5, ChronoUnit.MINUTES))));
            }
        );

        assertTrue(putTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        waitUntilCheckpoint(config.getId(), 1L);

        stopTransform(config.getId());
        assertBusy(() -> {
            assertEquals(TransformStats.State.STOPPED, getTransformStats(config.getId()).getTransformsStats().get(0).getState());
        });

        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            restClient.indices().refresh(new RefreshRequest(destIndexName), RequestOptions.DEFAULT);
            // Verify destination index mappings
            GetMappingsResponse destIndexMapping =
                restClient.indices().getMapping(new GetMappingsRequest().indices(destIndexName), RequestOptions.DEFAULT);
            assertThat(destIndexMapping.mappings().get(destIndexName).sourceAsMap(), allOf(hasKey("_meta"), hasKey("properties")));
            // Verify destination index contents
            SearchResponse searchResponse =
                restClient.search(new SearchRequest(destIndexName).source(new SearchSourceBuilder().size(1000)), RequestOptions.DEFAULT);
            assertThat(searchResponse.getHits().getTotalHits().value, is(equalTo(Long.valueOf(NUM_USERS))));
            assertThat(
                Stream.of(searchResponse.getHits().getHits()).map(SearchHit::getSourceAsMap).collect(toList()),
                is(equalTo(previewResponse.getDocs())));
        }
    }

    public void testPivotTransform_BadRuntimeFieldScript() throws Exception {
        String destIndexName = "reviews-by-user-pivot";
        String transformId = "transform-with-st-rt-fields-pivot";
        Map<String, Object> runtimeMappings = new HashMap<>() {{
            put("user-upper", new HashMap<>() {{
                put("type", "keyword");
                // Method name used in the script is misspelled, i.e.: "toUperCase" instead of "toUpperCase"
                put("script", singletonMap("source", "if (params._source.user_id != null) {emit(params._source.user_id.toUperCase())}"));
            }});
        }};

        Map<String, SingleGroupSource> groups = singletonMap("by-user", TermsGroupSource.builder().setField("user-upper").build());
        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.avg("review_score_rt").field("stars-x2"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"))
            .addAggregator(AggregationBuilders.max("timestamp_rt").field("timestamp-5m"));
        TransformConfig config =
            createTransformConfigBuilder(transformId, destIndexName, QueryBuilders.matchAllQuery(), "dummy")
                .setSource(SourceConfig.builder()
                    .setIndex(REVIEWS_INDEX_NAME)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setRuntimeMappings(runtimeMappings)
                    .build())
                .setPivotConfig(createPivotConfig(groups, aggs))
                .build();

        Exception e = expectThrows(Exception.class, () -> previewTransform(config, RequestOptions.DEFAULT));
        assertThat(
            ExceptionsHelper.stackTrace(e),
            allOf(containsString("script_exception"), containsString("dynamic method [java.lang.String, toUperCase/0] not found")));

        e = expectThrows(Exception.class, () -> putTransform(config, RequestOptions.DEFAULT));
        assertThat(
            ExceptionsHelper.stackTrace(e),
            allOf(containsString("script_exception"), containsString("dynamic method [java.lang.String, toUperCase/0] not found")));
    }

    public void testLatestTransform() throws Exception {
        String destIndexName = "reviews-by-user-latest";
        String transformId = "transform-with-st-rt-fields-latest";
        Map<String, Object> runtimeMappings = createRuntimeMappings();

        SourceConfig sourceConfig =
            SourceConfig.builder()
                .setIndex(REVIEWS_INDEX_NAME)
                .setQuery(QueryBuilders.matchAllQuery())
                .setRuntimeMappings(runtimeMappings)
                .build();
        TransformConfig configWithOrdinaryFields =
            createTransformConfigBuilder(transformId, destIndexName, QueryBuilders.matchAllQuery(), "dummy")
                .setSource(sourceConfig)
                .setLatestConfig(LatestConfig.builder()
                    .setUniqueKey("user_id")
                    .setSort("timestamp")
                    .build())
                .build();

        PreviewTransformResponse previewWithOrdinaryFields = previewTransform(configWithOrdinaryFields, RequestOptions.DEFAULT);
        // Verify preview mappings
        assertThat(previewWithOrdinaryFields.getMappings(), allOf(hasKey("_meta"), hasKey("properties")));
        // Verify preview contents
        assertThat("Got preview: " + previewWithOrdinaryFields, previewWithOrdinaryFields.getDocs(), hasSize(NUM_USERS));
        previewWithOrdinaryFields.getDocs().forEach(
            doc -> {
                assertThat(doc, hasKey("user_id"));
                assertThat(doc, not(hasKey("user-upper")));
            }
        );

        TransformConfig configWithRuntimeFields =
            createTransformConfigBuilder(transformId, destIndexName, QueryBuilders.matchAllQuery(), "dummy")
                .setSource(sourceConfig)
                .setLatestConfig(LatestConfig.builder()
                    .setUniqueKey("user-upper")
                    .setSort("timestamp-5m")
                    .build())
                .build();

        PreviewTransformResponse previewWithRuntimeFields = previewTransform(configWithRuntimeFields, RequestOptions.DEFAULT);
        assertThat(previewWithRuntimeFields.getDocs(), is(equalTo(previewWithOrdinaryFields.getDocs())));

        assertTrue(putTransform(configWithRuntimeFields, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(configWithRuntimeFields.getId(), RequestOptions.DEFAULT).isAcknowledged());

        waitUntilCheckpoint(configWithRuntimeFields.getId(), 1L);

        stopTransform(configWithRuntimeFields.getId());
        assertBusy(() -> {
            assertEquals(
                TransformStats.State.STOPPED,
                getTransformStats(configWithRuntimeFields.getId()).getTransformsStats().get(0).getState());
        });

        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            restClient.indices().refresh(new RefreshRequest(destIndexName), RequestOptions.DEFAULT);
            // Verify destination index mappings
            GetMappingsResponse destIndexMapping =
                restClient.indices().getMapping(new GetMappingsRequest().indices(destIndexName), RequestOptions.DEFAULT);
            assertThat(destIndexMapping.mappings().get(destIndexName).sourceAsMap(), allOf(hasKey("_meta"), hasKey("properties")));
            // Verify destination index contents
            SearchResponse searchResponse =
                restClient.search(new SearchRequest(destIndexName).source(new SearchSourceBuilder().size(1000)), RequestOptions.DEFAULT);
            assertThat(searchResponse.getHits().getTotalHits().value, is(equalTo(Long.valueOf(NUM_USERS))));
            assertThat(
                Stream.of(searchResponse.getHits().getHits()).map(SearchHit::getSourceAsMap).collect(toList()),
                is(equalTo(previewWithOrdinaryFields.getDocs())));
        }
    }

    public void testLatestTransform_BadRuntimeFieldScript() throws Exception {
        String destIndexName = "reviews-by-user-latest";
        String transformId = "transform-with-st-rt-fields-latest";
        Map<String, Object> runtimeMappings = new HashMap<>() {{
            put("user-upper", new HashMap<>() {{
                put("type", "keyword");
                // Method name used in the script is misspelled, i.e.: "toUperCase" instead of "toUpperCase"
                put("script", singletonMap("source", "if (params._source.user_id != null) {emit(params._source.user_id.toUperCase())}"));
            }});
        }};

        SourceConfig sourceConfig =
            SourceConfig.builder()
                .setIndex(REVIEWS_INDEX_NAME)
                .setQuery(QueryBuilders.matchAllQuery())
                .setRuntimeMappings(runtimeMappings)
                .build();
        TransformConfig configWithRuntimeFields =
            createTransformConfigBuilder(transformId, destIndexName, QueryBuilders.matchAllQuery(), "dummy")
                .setSource(sourceConfig)
                .setLatestConfig(LatestConfig.builder()
                    .setUniqueKey("user-upper")
                    .setSort("timestamp")
                    .build())
                .build();

        Exception e = expectThrows(Exception.class, () -> previewTransform(configWithRuntimeFields, RequestOptions.DEFAULT));
        assertThat(
            ExceptionsHelper.stackTrace(e),
            allOf(containsString("script_exception"), containsString("dynamic method [java.lang.String, toUperCase/0] not found")));

        e = expectThrows(Exception.class, () -> putTransform(configWithRuntimeFields, RequestOptions.DEFAULT));
        assertThat(
            ExceptionsHelper.stackTrace(e),
            allOf(containsString("script_exception"), containsString("dynamic method [java.lang.String, toUperCase/0] not found")));
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
