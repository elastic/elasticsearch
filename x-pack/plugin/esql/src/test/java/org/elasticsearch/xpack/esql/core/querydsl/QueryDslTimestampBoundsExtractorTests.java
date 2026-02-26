/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;

import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class QueryDslTimestampBoundsExtractorTests extends ESTestCase {

    public void testExtractTimestampBoundsFromRangeQuery() {
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-02T00:00:00Z");
        var filter = new RangeQueryBuilder("@timestamp").format("epoch_millis").gte(start.toEpochMilli()).lte(end.toEpochMilli());

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(start));
        assertThat(bounds.end(), equalTo(end));
    }

    public void testExtractTimestampBoundsFromBoolQuery() {
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-02T00:00:00Z");
        var rangeFilter = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").gte(start.toString()).lte(end.toString());
        var boolFilter = new BoolQueryBuilder().filter(rangeFilter).filter(new TermQueryBuilder("status", "active"));

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(boolFilter);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(start));
        assertThat(bounds.end(), equalTo(end));
    }

    public void testExtractTimestampBoundsFromNestedBoolQuery() {
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-02T00:00:00Z");
        var rangeFilter = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").gte(start.toString()).lte(end.toString());
        var innerBool = new BoolQueryBuilder().must(rangeFilter);
        var outerBool = new BoolQueryBuilder().filter(innerBool);

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(outerBool);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(start));
        assertThat(bounds.end(), equalTo(end));
    }

    public void testExtractTimestampBoundsNoTimestampField() {
        var filter = new RangeQueryBuilder("other_field").format("strict_date_optional_time").gte(100).lte(200);

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, nullValue());
    }

    public void testExtractTimestampBoundsNullFilter() {
        assertThat(QueryDslTimestampBoundsExtractor.extractTimestampBounds(null), nullValue());
    }

    public void testExtractTimestampBoundsFromStringDates() {
        var filter = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
            .gte("2025-01-01T00:00:00Z")
            .lte("2025-01-02T00:00:00Z");

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(Instant.parse("2025-01-01T00:00:00Z")));
        assertThat(bounds.end(), equalTo(Instant.parse("2025-01-02T00:00:00Z")));
    }

    public void testExtractTimestampBoundsDateMathDoesNotThrow() {
        var filter = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").gte("now-15m").lte("now");

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, nullValue());
    }

    public void testExtractTimestampBoundsInvalidValueDoesNotThrow() {
        var filter = new RangeQueryBuilder("@timestamp").format("epoch_millis").gte("not_a_timestamp").lte("1735776000000");

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, nullValue());
    }

}
