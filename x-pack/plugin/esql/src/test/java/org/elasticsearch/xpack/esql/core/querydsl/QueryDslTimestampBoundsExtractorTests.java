/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.BoundSemantics;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.dsltranslate.QueryDslTranslator;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class QueryDslTimestampBoundsExtractorTests extends ESTestCase {

    private static final long EPOCH_SECOND_2024_06_15 = 1_718_409_600L;
    private static final long EPOCH_SECOND_2024_06_16 = 1_718_496_000L;

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

    /**
     * RANGE_QUERY listing bounds contain the translator's closed {@code MV_IN_RANGE} interval, or the extractor
     * returns null when the translator drops the clause. Listing may be slightly wider (exclusive DSL is injected
     * as closed GTE/LTE); it must never be tighter.
     */
    public void testRangeQueryBoundsContainTranslatorOrNullWhenDropped() {
        Instant now = Instant.parse("2020-06-15T12:00:00Z");
        LongSupplier nowSupplier = now::toEpochMilli;
        Configuration config = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).now(now).build();
        Function<String, Expression> binder = name -> "@timestamp".equals(name)
            ? new ReferenceAttribute(Source.EMPTY, "@timestamp", DataType.DATETIME)
            : Literal.NULL;
        Set<String> fields = Set.of("@timestamp");

        record Case(String name, QueryBuilder filter) {}
        List<Case> cases = List.of(
            new Case("coarse round", new RangeQueryBuilder("@timestamp").gte("2020-06-15").lte("2020-06-16")),
            new Case("now", new RangeQueryBuilder("@timestamp").gte("now-15m").lte("now")),
            new Case("now/d", new RangeQueryBuilder("@timestamp").gte("now/d").lte("now/d")),
            new Case("exclusive", new RangeQueryBuilder("@timestamp").gt("2020-06-15T00:00:00.000Z").lt("2020-06-15T01:00:00.000Z")),
            new Case("coarse exclusive", new RangeQueryBuilder("@timestamp").gt("2020-06-15").lt("2020-06-17")),
            new Case(
                "format",
                new RangeQueryBuilder("@timestamp").format("strict_date_optional_time")
                    .gte("2024-06-15T00:00:00Z")
                    .lte("2024-06-15T01:00:00Z")
            ),
            new Case(
                "time_zone",
                new RangeQueryBuilder("@timestamp").timeZone("+02:00").gte("2025-01-01T00:00:00").lt("2025-01-02T00:00:00")
            ),
            new Case(
                "epoch_second",
                new RangeQueryBuilder("@timestamp").format("epoch_second").gte(EPOCH_SECOND_2024_06_15).lte(EPOCH_SECOND_2024_06_16)
            ),
            new Case("numeric no format", new RangeQueryBuilder("@timestamp").gte(2024L).lte(2025L)),
            new Case(
                "partial bool",
                new BoolQueryBuilder().filter(new RangeQueryBuilder("@timestamp").gte("2020-06-15").lte("2020-06-16"))
                    .filter(new RangeQueryBuilder("@timestamp").timeZone("+02:00").gte("2020-06-15").lte("2020-06-16"))
            )
        );
        for (Case c : cases) {
            TimestampBounds listing = QueryDslTimestampBoundsExtractor.extractTimestampBounds(
                c.filter(),
                nowSupplier,
                BoundSemantics.RANGE_QUERY
            );
            QueryDslTranslator.TranslationResult translated = new QueryDslTranslator(binder, fields, config, TransportVersion.current())
                .translate(c.filter());
            MvInRange appliedRange = timestampInRange(translated.applied());
            if (appliedRange == null) {
                assertThat(c.name(), listing, nullValue());
                continue;
            }
            long lo = (Long) ((Literal) appliedRange.lower()).value();
            long hi = (Long) ((Literal) appliedRange.upper()).value();
            assertThat(c.name(), listing, notNullValue());
            assertThat(c.name() + " lo", listing.start().toEpochMilli(), lessThanOrEqualTo(lo));
            assertThat(c.name() + " hi", listing.end().toEpochMilli(), greaterThanOrEqualTo(hi));
        }
    }

    public void testRangeQueryNumericEpochSecondIs2024Not1970() {
        var filter = new RangeQueryBuilder("@timestamp").format("epoch_second").gte(EPOCH_SECOND_2024_06_15).lte(EPOCH_SECOND_2024_06_16);
        TimestampBounds bounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter, null, BoundSemantics.RANGE_QUERY);
        assertThat(bounds, notNullValue());
        assertThat(bounds.start(), equalTo(Instant.parse("2024-06-15T00:00:00Z")));
        // lte rounds up through the last nano of that second — 1718496000 as millis would be 1970-01-20.
        assertThat(bounds.end(), equalTo(Instant.parse("2024-06-16T00:00:00.999999999Z")));
    }

    public void testRangeQueryNumericWithoutFormatIsEpochMillisNotYear() {
        var filter = new RangeQueryBuilder("@timestamp").gte(2024L).lte(2025L);
        TimestampBounds listing = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter, null, BoundSemantics.RANGE_QUERY);
        TimestampBounds legacy = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        assertThat(listing, notNullValue());
        assertThat(legacy, notNullValue());
        assertThat(listing.start(), equalTo(Instant.ofEpochMilli(2024)));
        assertThat(listing.end(), equalTo(Instant.ofEpochMilli(2025)));
        assertThat(legacy.start(), equalTo(Instant.parse("2024-01-01T00:00:00Z")));
    }

    public void testRangeQueryCoarseLteRoundsUpUnlikeLegacy() {
        var filter = new RangeQueryBuilder("@timestamp").gte("2020-06-15").lte("2020-06-15");
        TimestampBounds legacy = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        TimestampBounds listing = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter, null, BoundSemantics.RANGE_QUERY);
        assertThat(legacy, notNullValue());
        assertThat(listing, notNullValue());
        assertThat(legacy.start(), equalTo(Instant.parse("2020-06-15T00:00:00Z")));
        assertThat(legacy.end(), equalTo(Instant.parse("2020-06-15T00:00:00Z")));
        assertThat(listing.start(), equalTo(Instant.parse("2020-06-15T00:00:00Z")));
        assertThat(listing.end(), equalTo(Instant.parse("2020-06-15T23:59:59.999999999Z")));
    }

    public void testRangeQueryCoarseGtRoundsUpUnlikeLegacy() {
        var filter = new RangeQueryBuilder("@timestamp").gt("2020-06-15").lt("2020-06-17");
        TimestampBounds legacy = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter);
        TimestampBounds listing = QueryDslTimestampBoundsExtractor.extractTimestampBounds(filter, null, BoundSemantics.RANGE_QUERY);
        assertThat(legacy, notNullValue());
        assertThat(listing, notNullValue());
        assertThat(legacy.start(), equalTo(Instant.parse("2020-06-15T00:00:00Z")));
        assertThat(listing.start(), equalTo(Instant.parse("2020-06-15T23:59:59.999999999Z")));
    }

    @Nullable
    private static MvInRange timestampInRange(Expression applied) {
        return switch (applied) {
            case MvInRange range -> range;
            case And and -> {
                MvInRange left = timestampInRange(and.left());
                yield left != null ? left : timestampInRange(and.right());
            }
            default -> null;
        };
    }

}
