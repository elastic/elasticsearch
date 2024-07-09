/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.logsdb.qa.adaptors.MatcherAdaptor;
import org.elasticsearch.datastreams.logsdb.qa.exceptions.MatcherException;
import org.elasticsearch.datastreams.logsdb.qa.matchers.Matcher;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class StandardVersusLogsIndexModeChallengeIT extends AbstractChallengeTest {

    public StandardVersusLogsIndexModeChallengeIT() {
        super("oracle-data-stream", "challenge-data-stream", "oracle-template", "challenge-template");
    }

    private static void mappings(final XContentBuilder builder) throws IOException {
        builder.startObject()
            .field("subobjects", false)
            .startObject("properties")

            .startObject("@timestamp")
            .field("type", "date")
            .endObject()

            .startObject("host.name")
            .field("type", "keyword")
            .field("ignore_above", 1024)
            .endObject()

            .startObject("message")
            .field("type", "keyword")
            .field("ignore_above", 1024)
            .endObject()

            .startObject("method")
            .field("type", "keyword")
            .field("ignore_above", 1024)
            .endObject()

            .startObject("memory_usage_bytes")
            .field("type", "long")
            .field("ignore_malformed", true)
            .endObject()

            .endObject() // properties
            .endObject();
    }

    @Override
    public void oracleMappings(final XContentBuilder builder) throws IOException {
        mappings(builder);
    }

    @Override
    public void challengeMappings(XContentBuilder builder) throws IOException {
        mappings(builder);
    }

    @Override
    public void challengeSettings(Settings.Builder builder) {
        builder.put("index.mode", "logs");
    }

    public void testMatchAllQuery() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field(
                        "@timestamp",
                        DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName())
                            .format(Instant.now().plus(i, ChronoUnit.SECONDS))
                    )
                    .field("host.name", randomFrom("foo", "bar", "baz"))
                    .field("message", randomFrom("a message", "another message", "still another message", "one more message"))
                    .field("method", randomFrom("put", "post", "get"))
                    .field("memory_usage_bytes", randomLongBetween(1000, 2000))
                    .endObject()
            );
        }

        final Tuple<BulkResponse, BulkResponse> tuple = indexDocuments(() -> documents, () -> documents);
        refresh();
        assertThat(tuple.v1().hasFailures(), Matchers.equalTo(false));
        assertThat(tuple.v2().hasFailures(), Matchers.equalTo(tuple.v1().hasFailures()));

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments);

        SearchResponse oracleResponse = null;
        SearchResponse challengeResponse = null;
        try {
            oracleResponse = queryOracle(searchSourceBuilder);
            challengeResponse = queryChallenge(searchSourceBuilder);
            MatcherAdaptor<SearchHit[], Object[]> adaptor = hits -> Arrays.stream(hits)
                .map(searchHit -> Objects.requireNonNull(searchHit.getSourceAsMap()))
                .toArray();

            Matcher.mappings(getChallengeMappings(), getOracleMappings())
                .settings(getChallengeSettings(), getOracleSettings())
                .actual(adaptor.adapt(oracleResponse.getHits().getHits()))
                .expected(adaptor.adapt(challengeResponse.getHits().getHits()))
                .ignoreSorting(true)
                .isEqual();
        } finally {
            if (oracleResponse != null) {
                oracleResponse.decRef();
            }
            if (challengeResponse != null) {
                challengeResponse.decRef();
            }
        }
    }

    public void testTermsQuery() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            final String method = randomFrom("put", "post", "get");
            documents.add(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field(
                        "@timestamp",
                        DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName())
                            .format(Instant.now().plus(i, ChronoUnit.SECONDS))
                    )
                    .field("host.name", randomFrom("foo", "bar", "baz"))
                    .field("message", randomFrom("a message", "another message", "still another message", "one more message"))
                    .field("method", method)
                    .field("memory_usage_bytes", randomLongBetween(1000, 2000))
                    .endObject()
            );
        }

        final Tuple<BulkResponse, BulkResponse> tuple = indexDocuments(() -> documents, () -> documents);
        refresh();
        assertThat(tuple.v1().hasFailures(), Matchers.equalTo(false));
        assertThat(tuple.v2().hasFailures(), Matchers.equalTo(tuple.v1().hasFailures()));

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("method", "put"))
            .size(numberOfDocuments)
            .size(numberOfDocuments);
        SearchResponse oracleResponse = null;
        SearchResponse challengeResponse = null;
        try {
            oracleResponse = queryOracle(searchSourceBuilder);
            challengeResponse = queryChallenge(searchSourceBuilder);
            MatcherAdaptor<SearchHit[], Object[]> adaptor = hits -> Arrays.stream(hits)
                .map(searchHit -> Objects.requireNonNull(searchHit.getSourceAsMap()))
                .toArray();

            Matcher.mappings(getChallengeMappings(), getOracleMappings())
                .settings(getChallengeSettings(), getOracleSettings())
                .actual(adaptor.adapt(oracleResponse.getHits().getHits()))
                .expected(adaptor.adapt(challengeResponse.getHits().getHits()))
                .ignoreSorting(true)
                .isEqual();
        } finally {
            if (oracleResponse != null) {
                oracleResponse.decRef();
            }
            if (challengeResponse != null) {
                challengeResponse.decRef();
            }
        }
    }

    public void testHistogramAggregation() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field(
                        "@timestamp",
                        DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName())
                            .format(Instant.now().plus(i, ChronoUnit.SECONDS))
                    )
                    .field("host.name", randomFrom("foo", "bar", "baz"))
                    .field("message", randomFrom("a message", "another message", "still another message", "one more message"))
                    .field("method", randomFrom("put", "post", "get"))
                    .field("memory_usage_bytes", randomLongBetween(1000, 2000))
                    .endObject()
            );
        }

        final Tuple<BulkResponse, BulkResponse> tuple = indexDocuments(() -> documents, () -> documents);
        refresh();
        assertThat(tuple.v1().hasFailures(), Matchers.equalTo(false));
        assertThat(tuple.v2().hasFailures(), Matchers.equalTo(tuple.v1().hasFailures()));

        final HistogramAggregationBuilder histogramAggregation = new HistogramAggregationBuilder("agg").field("memory_usage_bytes");
        histogramAggregation.interval(100.0D);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments)
            .aggregation(histogramAggregation);
        SearchResponse oracleResponse = null;
        SearchResponse challengeResponse = null;
        try {
            oracleResponse = queryOracle(searchSourceBuilder);
            challengeResponse = queryChallenge(searchSourceBuilder);

            Matcher.mappings(getChallengeMappings(), getOracleMappings())
                .settings(getChallengeSettings(), getOracleSettings())
                .actual(oracleResponse.getAggregations().get("agg"))
                .expected(challengeResponse.getAggregations().get("agg"))
                .ignoreSorting(true)
                .isEqual();
        } finally {
            if (oracleResponse != null) {
                oracleResponse.decRef();
            }
            if (challengeResponse != null) {
                challengeResponse.decRef();
            }
        }
    }

    public void testTermsAggregation() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field(
                        "@timestamp",
                        DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName())
                            .format(Instant.now().plus(i, ChronoUnit.SECONDS))
                    )
                    .field("host.name", randomFrom("foo", "bar", "baz"))
                    .field("message", randomFrom("a message", "another message", "still another message", "one more message"))
                    .field("method", randomFrom("put", "post", "get"))
                    .field("memory_usage_bytes", randomLongBetween(1000, 2000))
                    .endObject()
            );
        }

        final Tuple<BulkResponse, BulkResponse> tuple = indexDocuments(() -> documents, () -> documents);
        refresh();
        assertThat(tuple.v1().hasFailures(), Matchers.equalTo(false));
        assertThat(tuple.v2().hasFailures(), Matchers.equalTo(tuple.v1().hasFailures()));

        final TermsAggregationBuilder termsAggregation = new TermsAggregationBuilder("agg").field("host.name");
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .size(numberOfDocuments)
            .size(0)
            .aggregation(termsAggregation);
        SearchResponse oracleResponse = null;
        SearchResponse challengeResponse = null;
        try {
            oracleResponse = queryOracle(searchSourceBuilder);
            challengeResponse = queryChallenge(searchSourceBuilder);

            Matcher.mappings(getChallengeMappings(), getOracleMappings())
                .settings(getChallengeSettings(), getOracleSettings())
                .actual(oracleResponse.getAggregations().get("agg"))
                .expected(challengeResponse.getAggregations().get("agg"))
                .ignoreSorting(true)
                .isEqual();
        } finally {
            if (oracleResponse != null) {
                oracleResponse.decRef();
            }
            if (challengeResponse != null) {
                challengeResponse.decRef();
            }
        }
    }

    public void testDateHistogramAggregation() throws IOException, MatcherException {
        final List<XContentBuilder> documents = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(100, 200);
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field(
                        "@timestamp",
                        DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName())
                            .format(Instant.now().plus(i, ChronoUnit.SECONDS))
                    )
                    .field("host.name", randomFrom("foo", "bar", "baz"))
                    .field("message", randomFrom("a message", "another message", "still another message", "one more message"))
                    .field("method", randomFrom("put", "post", "get"))
                    .field("memory_usage_bytes", randomLongBetween(1000, 2000))
                    .endObject()
            );
        }

        final Tuple<BulkResponse, BulkResponse> tuple = indexDocuments(() -> documents, () -> documents);
        refresh();
        assertThat(tuple.v1().hasFailures(), Matchers.equalTo(false));
        assertThat(tuple.v2().hasFailures(), Matchers.equalTo(tuple.v1().hasFailures()));

        final DateHistogramAggregationBuilder dateHisto = AggregationBuilders.dateHistogram("agg")
            .field("@timestamp")
            .calendarInterval(DateHistogramInterval.SECOND);

        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .aggregation(dateHisto)
            .size(0);
        SearchResponse oracleResponse = null;
        SearchResponse challengeResponse = null;
        try {
            oracleResponse = queryOracle(sourceBuilder);
            challengeResponse = queryChallenge(sourceBuilder);

            Matcher.mappings(getChallengeMappings(), getOracleMappings())
                .settings(getChallengeSettings(), getOracleSettings())
                .actual(oracleResponse.getAggregations().get("agg"))
                .expected(challengeResponse.getAggregations().get("agg"))
                .ignoreSorting(true)
                .isEqual();
        } finally {
            if (oracleResponse != null) {
                oracleResponse.decRef();
            }
            if (challengeResponse != null) {
                challengeResponse.decRef();
            }
        }
    }

}
