/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper.RangeFieldType;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.BinaryDocValuesRangeQuery;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class RangeFieldTypeTests extends FieldTypeTestCase {
    RangeType type;
    protected static int DISTANCE = 10;
    private static long nowInMillis;

    @Before
    public void setupProperties() {
        type = randomFrom(RangeType.values());
        nowInMillis = randomNonNegativeLong();
    }

    private RangeFieldType createDefaultFieldType() {
        if (type == RangeType.DATE) {
            return new RangeFieldType("field", RangeFieldMapper.Defaults.DATE_FORMATTER);
        }
        return new RangeFieldType("field", type);
    }

    public void testRangeQuery() throws Exception {
        SearchExecutionContext context = createContext();
        RangeFieldType ft = createDefaultFieldType();

        ShapeRelation relation = randomFrom(ShapeRelation.values());
        boolean includeLower = randomBoolean();
        boolean includeUpper = randomBoolean();
        Object from = nextFrom();
        Object to = nextTo(from);
        if (includeLower == false && includeUpper == false) {
            // need to increase once more, otherwise interval is empty because edge values are exclusive
            to = nextTo(to);
        }

        assertEquals(
            getExpectedRangeQuery(relation, from, to, includeLower, includeUpper),
            ft.rangeQuery(from, to, includeLower, includeUpper, relation, null, null, context)
        );
    }

    /**
     * test the queries are correct if from/to are adjacent and the range is exclusive of those values
     */
    public void testRangeQueryIntersectsAdjacentValues() throws Exception {
        SearchExecutionContext context = createContext();
        ShapeRelation relation = randomFrom(ShapeRelation.values());
        RangeFieldType ft = createDefaultFieldType();

        Object from;
        Object to;
        switch (type) {
            case LONG -> {
                long fromValue = randomLong();
                from = fromValue;
                to = fromValue + 1;
            }
            case DATE -> {
                long fromValue = randomInt();
                from = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromValue), ZoneOffset.UTC);
                to = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromValue + 1), ZoneOffset.UTC);
            }
            case INTEGER -> {
                int fromValue = randomInt();
                from = fromValue;
                to = fromValue + 1;
            }
            case DOUBLE -> {
                double fromValue = randomDoubleBetween(0, 100, true);
                from = fromValue;
                to = Math.nextUp(fromValue);
            }
            case FLOAT -> {
                float fromValue = randomFloat();
                from = fromValue;
                to = Math.nextUp(fromValue);
            }
            case IP -> {
                byte[] ipv4 = new byte[4];
                random().nextBytes(ipv4);
                InetAddress fromValue = InetAddress.getByAddress(ipv4);
                from = fromValue;
                to = InetAddressPoint.nextUp(fromValue);
            }
            default -> {
                from = nextFrom();
                to = nextTo(from);
            }
        }
        Query rangeQuery = ft.rangeQuery(from, to, false, false, relation, null, null, context);
        assertThat(rangeQuery, instanceOf(IndexOrDocValuesQuery.class));
        assertThat(((IndexOrDocValuesQuery) rangeQuery).getIndexQuery(), instanceOf(MatchNoDocsQuery.class));
    }

    /**
     * check that we catch cases where the user specifies larger "from" than "to" value, not counting the include upper/lower settings
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/86284")
    public void testFromLargerToErrors() throws Exception {
        SearchExecutionContext context = createContext();
        RangeFieldType ft = createDefaultFieldType();

        final Object from;
        final Object to;
        switch (type) {
            case LONG: {
                long fromValue = randomLong();
                from = fromValue;
                to = fromValue - 1L;
                break;
            }
            case DATE: {
                long fromValue = randomInt();
                from = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromValue), ZoneOffset.UTC);
                to = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromValue - 1), ZoneOffset.UTC);
                break;
            }
            case INTEGER: {
                int fromValue = randomInt();
                from = fromValue;
                to = fromValue - 1;
                break;
            }
            case DOUBLE: {
                double fromValue = randomDoubleBetween(0, 100, true);
                from = fromValue;
                to = fromValue - 1.0d;
                break;
            }
            case FLOAT: {
                float fromValue = randomFloat();
                from = fromValue;
                to = fromValue - 1.0f;
                break;
            }
            case IP: {
                byte[] ipv4 = new byte[4];
                random().nextBytes(ipv4);
                InetAddress fromValue = InetAddress.getByAddress(ipv4);
                from = fromValue;
                to = InetAddressPoint.nextDown(fromValue);
                break;
            }
            default:
                // quit test for other range types
                return;
        }
        ShapeRelation relation = randomFrom(ShapeRelation.values());
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ft.rangeQuery(from, to, true, true, relation, null, null, context)
        );
        assertTrue(ex.getMessage().contains("Range query `from` value"));
        assertTrue(ex.getMessage().contains("is greater than `to` value"));
    }

    private SearchExecutionContext createContext() {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);
        return new SearchExecutionContext(
            0,
            0,
            idxSettings,
            null,
            null,
            null,
            MappingLookup.EMPTY,
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            null,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        );
    }

    public void testDateRangeQueryUsingMappingFormat() {
        SearchExecutionContext context = createContext();
        RangeFieldType strict = new RangeFieldType("field", RangeFieldMapper.Defaults.DATE_FORMATTER);
        // don't use DISJOINT here because it doesn't work on date fields which we want to compare bounds with
        ShapeRelation relation = randomValueOtherThan(ShapeRelation.DISJOINT, () -> randomFrom(ShapeRelation.values()));

        // dates will break the default format, month/day of month is turned around in the format
        final String from = "2016-15-06T15:29:50+08:00";
        final String to = "2016-16-06T15:29:50+08:00";

        ElasticsearchParseException ex = expectThrows(
            ElasticsearchParseException.class,
            () -> strict.rangeQuery(from, to, true, true, relation, null, null, context)
        );
        assertThat(
            ex.getMessage(),
            containsString("failed to parse date field [2016-15-06T15:29:50+08:00] with format [strict_date_optional_time||epoch_millis]")
        );

        // setting mapping format which is compatible with those dates
        final DateFormatter formatter = DateFormatter.forPattern("yyyy-dd-MM'T'HH:mm:ssZZZZZ");
        assertEquals(1465975790000L, formatter.parseMillis(from));
        assertEquals(1466062190000L, formatter.parseMillis(to));

        RangeFieldType fieldType = new RangeFieldType("field", formatter);
        final Query query = fieldType.rangeQuery(from, to, true, true, relation, null, fieldType.dateMathParser(), context);
        assertEquals("field:<ranges:[1465975790000 : 1466062190999]>", query.toString());

        // compare lower and upper bounds with what we would get on a `date` field
        DateFieldType dateFieldType = new DateFieldType("field", DateFieldMapper.Resolution.MILLISECONDS, formatter);
        final Query queryOnDateField = dateFieldType.rangeQuery(from, to, true, true, relation, null, fieldType.dateMathParser(), context);
        assertEquals("field:[1465975790000 TO 1466062190999]", queryOnDateField.toString());
    }

    /**
     * We would like to ensure lower and upper bounds are consistent between queries on a `date` and a`date_range`
     * field, so we randomize a few cases and compare the generated queries here
     */
    public void testDateVsDateRangeBounds() {
        SearchExecutionContext context = createContext();

        // date formatter that truncates seconds, so we get some rounding behavior
        final DateFormatter formatter = DateFormatter.forPattern("yyyy-dd-MM'T'HH:mm");
        long lower = randomLongBetween(formatter.parseMillis("2000-01-01T00:00"), formatter.parseMillis("2010-01-01T00:00"));
        long upper = randomLongBetween(formatter.parseMillis("2011-01-01T00:00"), formatter.parseMillis("2020-01-01T00:00"));

        RangeFieldType fieldType = new RangeFieldType("field", true, false, false, formatter, false, Collections.emptyMap());
        String lowerAsString = formatter.formatMillis(lower);
        String upperAsString = formatter.formatMillis(upper);
        // also add date math rounding to days occasionally
        if (randomBoolean()) {
            lowerAsString = lowerAsString + "||/d";
        }
        if (randomBoolean()) {
            upperAsString = upperAsString + "||/d";
        }
        boolean includeLower = randomBoolean();
        boolean includeUpper = randomBoolean();
        final Query query = fieldType.rangeQuery(
            lowerAsString,
            upperAsString,
            includeLower,
            includeUpper,
            ShapeRelation.INTERSECTS,
            null,
            null,
            context
        );

        // get exact lower and upper bounds similar to what we would parse for `date` fields for same input strings
        DateFieldType dateFieldType = new DateFieldType("field");
        long lowerBoundLong = dateFieldType.parseToLong(lowerAsString, includeLower == false, null, formatter.toDateMathParser(), () -> 0);
        if (includeLower == false) {
            ++lowerBoundLong;
        }
        long upperBoundLong = dateFieldType.parseToLong(upperAsString, includeUpper, null, formatter.toDateMathParser(), () -> 0);
        if (includeUpper == false) {
            --upperBoundLong;
        }

        // check that using this bounds we get similar query when constructing equivalent query on date_range field
        Query range = LongRange.newIntersectsQuery("field", new long[] { lowerBoundLong }, new long[] { upperBoundLong });
        assertEquals(range, query);
    }

    private Query getExpectedRangeQuery(ShapeRelation relation, Object from, Object to, boolean includeLower, boolean includeUpper) {
        return switch (type) {
            case DATE -> getDateRangeQuery(relation, (ZonedDateTime) from, (ZonedDateTime) to, includeLower, includeUpper);
            case INTEGER -> getIntRangeQuery(relation, (int) from, (int) to, includeLower, includeUpper);
            case LONG -> getLongRangeQuery(relation, (long) from, (long) to, includeLower, includeUpper);
            case DOUBLE -> getDoubleRangeQuery(relation, (double) from, (double) to, includeLower, includeUpper);
            case IP -> getInetAddressRangeQuery(relation, (InetAddress) from, (InetAddress) to, includeLower, includeUpper);
            default -> getFloatRangeQuery(relation, (float) from, (float) to, includeLower, includeUpper);
        };
    }

    private Query getDateRangeQuery(
        ShapeRelation relation,
        ZonedDateTime from,
        ZonedDateTime to,
        boolean includeLower,
        boolean includeUpper
    ) {
        long[] lower = new long[] { from.toInstant().toEpochMilli() + (includeLower ? 0 : 1) };
        long[] upper = new long[] { to.toInstant().toEpochMilli() - (includeUpper ? 0 : 1) };
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = LongRange.newWithinQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = LongRange.newContainsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = LongRange.newIntersectsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.DATE.dvRangeQuery(
            "field",
            queryType,
            from.toInstant().toEpochMilli(),
            to.toInstant().toEpochMilli(),
            includeLower,
            includeUpper
        );
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getIntRangeQuery(ShapeRelation relation, int from, int to, boolean includeLower, boolean includeUpper) {
        int[] lower = new int[] { from + (includeLower ? 0 : 1) };
        int[] upper = new int[] { to - (includeUpper ? 0 : 1) };
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = IntRange.newWithinQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = IntRange.newContainsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = IntRange.newIntersectsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.INTEGER.dvRangeQuery("field", queryType, from, to, includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getLongRangeQuery(ShapeRelation relation, long from, long to, boolean includeLower, boolean includeUpper) {
        long[] lower = new long[] { from + (includeLower ? 0 : 1) };
        long[] upper = new long[] { to - (includeUpper ? 0 : 1) };
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = LongRange.newWithinQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = LongRange.newContainsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = LongRange.newIntersectsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.LONG.dvRangeQuery("field", queryType, from, to, includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getFloatRangeQuery(ShapeRelation relation, float from, float to, boolean includeLower, boolean includeUpper) {
        float[] lower = new float[] { includeLower ? from : Math.nextUp(from) };
        float[] upper = new float[] { includeUpper ? to : Math.nextDown(to) };
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = FloatRange.newWithinQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = FloatRange.newContainsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = FloatRange.newIntersectsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.FLOAT.dvRangeQuery("field", queryType, from, to, includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getDoubleRangeQuery(ShapeRelation relation, double from, double to, boolean includeLower, boolean includeUpper) {
        double[] lower = new double[] { includeLower ? from : Math.nextUp(from) };
        double[] upper = new double[] { includeUpper ? to : Math.nextDown(to) };
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = DoubleRange.newWithinQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = DoubleRange.newContainsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = DoubleRange.newIntersectsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.DOUBLE.dvRangeQuery("field", queryType, from, to, includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getInetAddressRangeQuery(
        ShapeRelation relation,
        InetAddress from,
        InetAddress to,
        boolean includeLower,
        boolean includeUpper
    ) {
        InetAddress lower = includeLower ? from : InetAddressPoint.nextUp(from);
        InetAddress upper = includeUpper ? to : InetAddressPoint.nextDown(to);
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = InetAddressRange.newWithinQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = InetAddressRange.newContainsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = InetAddressRange.newIntersectsQuery("field", lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.IP.dvRangeQuery("field", queryType, from, to, includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Object nextFrom() throws Exception {
        return switch (type) {
            case INTEGER -> (int) (random().nextInt() * 0.5 - DISTANCE);
            case DATE -> ZonedDateTime.now(ZoneOffset.UTC);
            case LONG -> (long) (random().nextLong() * 0.5 - DISTANCE);
            case FLOAT -> (float) (random().nextFloat() * 0.5 - DISTANCE);
            case IP -> InetAddress.getByName("::ffff:c0a8:107");
            default -> random().nextDouble() * 0.5 - DISTANCE;
        };
    }

    private Object nextTo(Object from) throws Exception {
        return switch (type) {
            case INTEGER -> (Integer) from + DISTANCE;
            case DATE -> ZonedDateTime.now(ZoneOffset.UTC).plusDays(DISTANCE);
            case LONG -> (Long) from + DISTANCE;
            case DOUBLE -> (Double) from + DISTANCE;
            case IP -> InetAddress.getByName("2001:db8::");
            default -> (Float) from + DISTANCE;
        };
    }

    public void testParseIp() {
        assertEquals(InetAddresses.forString("::1"), RangeType.IP.parseValue(InetAddresses.forString("::1"), randomBoolean(), null));
        assertEquals(InetAddresses.forString("::1"), RangeType.IP.parseValue("::1", randomBoolean(), null));
        assertEquals(InetAddresses.forString("::1"), RangeType.IP.parseValue(new BytesRef("::1"), randomBoolean(), null));
    }

    public void testTermQuery() throws Exception {
        // See https://github.com/elastic/elasticsearch/issues/25950
        SearchExecutionContext context = createContext();
        RangeFieldType ft = createDefaultFieldType();

        Object value = nextFrom();
        ShapeRelation relation = ShapeRelation.INTERSECTS;
        boolean includeLower = true;
        boolean includeUpper = true;
        assertEquals(getExpectedRangeQuery(relation, value, value, includeLower, includeUpper), ft.termQuery(value, context));
    }

    public void testCaseInsensitiveQuery() throws Exception {
        SearchExecutionContext context = createContext();
        RangeFieldType ft = createDefaultFieldType();

        Object value = nextFrom();
        QueryShardException ex = expectThrows(QueryShardException.class, () -> ft.termQueryCaseInsensitive(value, context));
        assertTrue(ex.getMessage().contains("does not support case insensitive term queries"));
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType longMapper = new RangeFieldMapper.Builder("field", RangeType.LONG, true).build(MapperBuilderContext.root(false))
            .fieldType();
        Map<String, Object> longRange = Map.of("gte", 3.14, "lt", "42.9");
        assertEquals(List.of(Map.of("gte", 3L, "lt", 42L)), fetchSourceValue(longMapper, longRange));

        MappedFieldType dateMapper = new RangeFieldMapper.Builder("field", RangeType.DATE, true).format("yyyy/MM/dd||epoch_millis")
            .build(MapperBuilderContext.root(false))
            .fieldType();
        Map<String, Object> dateRange = Map.of("lt", "1990/12/29", "gte", 597429487111L);
        assertEquals(List.of(Map.of("lt", "1990/12/29", "gte", "1988/12/06")), fetchSourceValue(dateMapper, dateRange));
    }

    public void testParseSourceValueWithFormat() throws IOException {
        MappedFieldType longMapper = new RangeFieldMapper.Builder("field", RangeType.LONG, true).build(MapperBuilderContext.root(false))
            .fieldType();
        Map<String, Object> longRange = Map.of("gte", 3.14, "lt", "42.9");
        assertEquals(List.of(Map.of("gte", 3L, "lt", 42L)), fetchSourceValue(longMapper, longRange));

        MappedFieldType dateMapper = new RangeFieldMapper.Builder("field", RangeType.DATE, true).format("strict_date_time")
            .build(MapperBuilderContext.root(false))
            .fieldType();
        Map<String, Object> dateRange = Map.of("lt", "1990-12-29T00:00:00.000Z");
        assertEquals(List.of(Map.of("lt", "1990/12/29")), fetchSourceValue(dateMapper, dateRange, "yyy/MM/dd"));
        assertEquals(List.of(Map.of("lt", "662428800000")), fetchSourceValue(dateMapper, dateRange, "epoch_millis"));
    }
}
