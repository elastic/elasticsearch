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

import java.time.Duration;
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

    public void testIgnoresRangeInShouldClause() {
        var range = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
            .gte("2025-01-01T00:00:00Z")
            .lte("2025-01-02T00:00:00Z");
        var filter = new BoolQueryBuilder().should(range);

        assertThat(QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter), nullValue());
    }

    public void testIgnoresRangeInMustNotClause() {
        var range = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
            .gte("2025-01-01T00:00:00Z")
            .lte("2025-01-02T00:00:00Z");
        var filter = new BoolQueryBuilder().mustNot(range);

        assertThat(QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter), nullValue());
    }

    public void testFilterRangeExtractedShouldRangeIgnored() {
        Instant filterStart = Instant.parse("2025-01-01T00:00:00Z");
        Instant filterEnd = Instant.parse("2025-01-02T00:00:00Z");
        var filterRange = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
            .gte(filterStart.toString())
            .lte(filterEnd.toString());
        var shouldRange = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
            .gte("2025-06-01T00:00:00Z")
            .lte("2025-06-30T00:00:00Z");
        var filter = new BoolQueryBuilder().filter(filterRange).should(shouldRange);

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(filterStart));
        assertThat(bounds.end(), equalTo(filterEnd));
    }

    public void testIgnoresRangeNestedInsideShouldSubtree() {
        var range = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
            .gte("2025-01-01T00:00:00Z")
            .lte("2025-01-02T00:00:00Z");
        var innerBool = new BoolQueryBuilder().should(range);
        var outerBool = new BoolQueryBuilder().must(innerBool);

        assertThat(QueryDslTimestampBoundsExtractor.extractTimestampBounds(outerBool), nullValue());
    }

    public void testExtractTimestampBoundsFromSplitRangeClauses() {
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-02T00:00:00Z");
        var lowerBound = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").gte(start.toString());
        var upperBound = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").lt(end.toString());
        var filter = new BoolQueryBuilder().filter(lowerBound).filter(upperBound);

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(start));
        assertThat(bounds.end(), equalTo(end));
    }

    public void testExtractTimestampBoundsUsesMostRestrictiveMatchingBounds() {
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant narrowedStart = Instant.parse("2025-01-01T12:00:00Z");
        Instant end = Instant.parse("2025-01-02T00:00:00Z");
        Instant narrowedEnd = Instant.parse("2025-01-01T18:00:00Z");
        var narrowedFilter = new BoolQueryBuilder().filter(
            new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").gte(narrowedStart.toString())
        ).filter(new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").lt(narrowedEnd.toString()));
        var filter = new BoolQueryBuilder().filter(
            new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").gte(start.toString()).lte(end.toString())
        ).must(narrowedFilter);

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(narrowedStart));
        assertThat(bounds.end(), equalTo(narrowedEnd));
    }

    public void testExtractTimestampBoundsUsesTimeZone() {
        var filter = new RangeQueryBuilder("@timestamp").timeZone("+02:00").gte("2025-01-01T00:00:00").lt("2025-01-02T00:00:00");

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(Instant.parse("2024-12-31T22:00:00Z")));
        assertThat(bounds.end(), equalTo(Instant.parse("2025-01-01T22:00:00Z")));
    }

    public void testExtractTimestampBoundsDateMathUsesSuppliedNow() {
        Instant now = Instant.parse("2025-01-02T12:00:00Z");
        var filter = new RangeQueryBuilder("@timestamp").gte("now-15m").lte("now");

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter, now::toEpochMilli);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(now.minus(Duration.ofMinutes(15))));
        assertThat(bounds.end(), equalTo(now));
    }

    public void testExtractTimestampBoundsWithoutExplicitFormat() {
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-02T00:00:00Z");
        var filter = new RangeQueryBuilder("@timestamp").gte(start.toString()).lte(end.toString());

        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(start));
        assertThat(bounds.end(), equalTo(end));
    }

    public void testExtractTimestampBoundsReturnsNullForInvertedRange() {
        // Intersecting two non-overlapping ranges produces start > end
        var wide = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
            .gte("2025-01-01T00:00:00Z")
            .lte("2025-01-01T12:00:00Z");
        var narrow = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
            .gte("2025-01-02T00:00:00Z")
            .lte("2025-01-02T12:00:00Z");
        var filter = new BoolQueryBuilder().filter(wide).filter(narrow);

        assertThat(QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter), nullValue());
    }

    public void testExtractTimestampBoundsReturnsNullForUnparseableValue() {
        var filter = new RangeQueryBuilder("@timestamp").format("strict_date_optional_time").gte("not-a-date").lte("also-not-a-date");

        assertThat(QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter), nullValue());
    }

    public void testExtractTimestampBoundsDateMathWithoutNowSupplierReturnsNull() {
        var filter = new RangeQueryBuilder("@timestamp").gte("now-15m").lte("now");

        // Without a nowSupplier, date math with "now" returns null
        assertThat(QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter), nullValue());
    }

}
