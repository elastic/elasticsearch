/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.queries.BinaryDocValuesRangeQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.RangeFieldMapper.RangeType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.IndexSettingsModule;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Locale;

public class RangeFieldTypeTests extends FieldTypeTestCase {
    RangeType type;
    protected static String FIELDNAME = "field";
    protected static int DISTANCE = 10;
    private static long nowInMillis;
    private static final DateMathParser WITHOUT_DATE_MATH_PARSER = null;

    @Before
    public void setupProperties() {
        type = RandomPicks.randomFrom(random(), RangeType.values());
        nowInMillis = randomNonNegativeLong();
        if (type == RangeType.DATE) {
            addModifier(new Modifier("format", true) {
                @Override
                public void modify(MappedFieldType ft) {
                    ((RangeFieldMapper.RangeFieldType) ft).setDateTimeFormatter(Joda.forPattern("basic_week_date", Locale.ROOT));
                }
            });
            addModifier(new Modifier("locale", true) {
                @Override
                public void modify(MappedFieldType ft) {
                    ((RangeFieldMapper.RangeFieldType) ft).setDateTimeFormatter(Joda.forPattern("date_optional_time", Locale.CANADA));
                }
            });
        }
    }

    @Override
    protected RangeFieldMapper.RangeFieldType createDefaultFieldType() {
        return new RangeFieldMapper.RangeFieldType(type, Version.CURRENT);
    }

    public void testRangeQuery() throws Exception {
        RangeFieldMapper.RangeFieldType ft = this.rangeFieldType(type);

        ShapeRelation relation = RandomPicks.randomFrom(random(), ShapeRelation.values());
        boolean includeLower = random().nextBoolean();
        boolean includeUpper = random().nextBoolean();
        Object from = nextFrom();
        Object to = nextTo(from);

        assertEquals(getExpectedRangeQuery(relation, from, to, includeLower, includeUpper),
            ft.rangeQuery(from, to, includeLower, includeUpper, relation, null, null, this.context()));
    }

    public void testRangeQueryDate_WithoutFormat_usesRangeFieldTypeFormat_includeLowerIncludeUpperTimeZone() {
        testRangeQueryDate_WithoutFormat_usesRangeFieldTypeFormat0(true, true, DateTimeZone.UTC);
    }

    public void testRangeQueryDate_WithoutFormat_usesRangeFieldTypeFormat() {
        testRangeQueryDate_WithoutFormat_usesRangeFieldTypeFormat0(false, false, null);
    }

    private void testRangeQueryDate_WithoutFormat_usesRangeFieldTypeFormat0(final boolean includeLower,
                                                                            final boolean includeUpper,
                                                                            final DateTimeZone timeZone) {
        final String pattern = "yyyy-MM-dd";
        final String lower = "2015-10-31";
        final String higher = "2016-11-01";

        final Query query = this.rangeQuery(RangeType.DATE,
            pattern,
            lower,
            higher,
            includeLower,
            includeUpper,
            ShapeRelation.WITHIN,
            timeZone,
            WITHOUT_DATE_MATH_PARSER);

        final DateTimeFormatter parser = this.parser(pattern, timeZone);

        Assert.assertEquals(this.getDateRangeQuery(
            ShapeRelation.WITHIN,
            parser.parseDateTime(lower),
            parser.parseDateTime(higher),
            includeLower,
            includeUpper),
            query);
    }

    // fails because lowerTerm/upperTerm cannt be parsed with dd-MM-yyyy
    public void testRangeQueryDate_WithoutFormat_usesRangeFieldTypeFormat_incompatFails() {
        this.rangeQueryFails(RangeType.DATE,
            "dd-yyyy-MM", //deliberate must cause failure
            "2015-10-31",
            "2016-11-01",
            true,
            true,
            ShapeRelation.WITHIN,
            DateTimeZone.UTC,
            WITHOUT_DATE_MATH_PARSER,
            ElasticsearchParseException.class);
    }

    public void testRangeQueryDate_WithFormat() {
        testRangeQueryDate_WithFormat0(false, false, null);
    }

    public void testRangeQueryDate_WithFormat_includeLowerIncludeUpperTimezone() {
        testRangeQueryDate_WithFormat0(true, true, DateTimeZone.UTC);
    }

    private void testRangeQueryDate_WithFormat0(final boolean includeLower,
                                                final boolean includeUpper,
                                                final DateTimeZone timeZone) {
        final String pattern = "yyyy-MM-dd";
        final String lower = "2015-10-31";
        final String higher = "2016-11-01";

        final Query query = this.rangeQuery(RangeType.DATE,
            pattern,
            lower,
            higher,
            includeLower,
            includeUpper,
            ShapeRelation.WITHIN,
            timeZone,
            dateMathParser(pattern));

        final DateTimeFormatter parser = this.parser(pattern, timeZone);

        Assert.assertEquals(this.getDateRangeQuery(
            ShapeRelation.WITHIN,
            parser.parseDateTime(lower),
            parser.parseDateTime(higher),
            includeLower,
            includeUpper),
            query);
    }

    private void rangeQueryFails(final RangeType rangeType,
                            final String dateTimeFormatterFormat,
                            final String lowerTerm,
                            final String upperTerm,
                            final boolean includeLower,
                            final boolean includeUpper,
                            final ShapeRelation relation,
                            final DateTimeZone dateTimeZone,
                            final DateMathParser dateMathParser,
                                 final Class<? extends Throwable> thrown)
    {
        expectThrows(thrown,
            () -> rangeQuery(rangeType,
                dateTimeFormatterFormat,
                lowerTerm,
                upperTerm,
                includeLower,
                includeUpper,
                relation,
                dateTimeZone,
                dateMathParser));
    }

    private Query rangeQuery(final RangeType rangeType,
                            final String dateTimeFormatterFormat,
                            final String lowerTerm,
                            final String upperTerm,
                            final boolean includeLower,
                            final boolean includeUpper,
                            final ShapeRelation relation,
                            final DateTimeZone dateTimeZone,
                            final DateMathParser dateMathParser)
    {
        final RangeFieldMapper.RangeFieldType type = rangeFieldType(rangeType, dateTimeFormatterFormat);

        return type.rangeQuery(
            lowerTerm,
            upperTerm,
            includeLower,
            includeUpper,
            relation,
            dateTimeZone,
            dateMathParser,
            this.context());
    }

    private RangeFieldMapper.RangeFieldType rangeFieldType(final RangeType rangeType) {
        return this.rangeFieldType(rangeType, null);
    }

    private RangeFieldMapper.RangeFieldType rangeFieldType(final RangeType rangeType,
                                                           final String dateTimeFormatterFormat) {
        final RangeFieldMapper.RangeFieldType rangeFieldType = new RangeFieldMapper.RangeFieldType(rangeType, Version.CURRENT);
        rangeFieldType.setName(FIELDNAME);
        rangeFieldType.setIndexOptions(IndexOptions.DOCS);

        if(null!=dateTimeFormatterFormat){
            rangeFieldType.setDateTimeFormatter(Joda.forPattern(dateTimeFormatterFormat));
        }
        return rangeFieldType;
    }

    private DateMathParser dateMathParser(final String pattern) {
        return new DateMathParser(formatter(pattern));
    }

    private FormatDateTimeFormatter formatter(final String pattern) {
        return Joda.forPattern(pattern, Locale.ROOT);
    }

    private DateTimeFormatter parser(final String pattern, final DateTimeZone timeZone) {
        DateTimeFormatter parser = formatter(pattern).parser();
        if(null !=timeZone){
            parser = parser.withZone(timeZone);
        }
        return parser;
    }

    private Query getExpectedRangeQuery(ShapeRelation relation, Object from, Object to, boolean includeLower, boolean includeUpper) {
        switch (type) {
            case DATE:
                return getDateRangeQuery(relation, (DateTime)from, (DateTime)to, includeLower, includeUpper);
            case INTEGER:
                return getIntRangeQuery(relation, (int)from, (int)to, includeLower, includeUpper);
            case LONG:
                return getLongRangeQuery(relation, (long)from, (long)to, includeLower, includeUpper);
            case DOUBLE:
                return getDoubleRangeQuery(relation, (double)from, (double)to, includeLower, includeUpper);
            case IP:
                return getInetAddressRangeQuery(relation, (InetAddress)from, (InetAddress)to, includeLower, includeUpper);
            default:
                return getFloatRangeQuery(relation, (float)from, (float)to, includeLower, includeUpper);
        }
    }

    private Query getDateRangeQuery(ShapeRelation relation, DateTime from, DateTime to, boolean includeLower, boolean includeUpper) {
        long[] lower = new long[] {from.getMillis() + (includeLower ? 0 : 1)};
        long[] upper = new long[] {to.getMillis() - (includeUpper ? 0 : 1)};
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = LongRange.newWithinQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = LongRange.newContainsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = LongRange.newIntersectsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.DATE.dvRangeQuery(FIELDNAME, queryType, from.getMillis(),
                to.getMillis(), includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getIntRangeQuery(ShapeRelation relation, int from, int to, boolean includeLower, boolean includeUpper) {
        int[] lower = new int[] {from + (includeLower ? 0 : 1)};
        int[] upper = new int[] {to - (includeUpper ? 0 : 1)};
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = IntRange.newWithinQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = IntRange.newContainsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = IntRange.newIntersectsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.INTEGER.dvRangeQuery(FIELDNAME, queryType, from, to,
                includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getLongRangeQuery(ShapeRelation relation, long from, long to, boolean includeLower, boolean includeUpper) {
        long[] lower = new long[] {from + (includeLower ? 0 : 1)};
        long[] upper = new long[] {to - (includeUpper ? 0 : 1)};
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = LongRange.newWithinQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = LongRange.newContainsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = LongRange.newIntersectsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.LONG.dvRangeQuery(FIELDNAME, queryType, from, to,
                includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getFloatRangeQuery(ShapeRelation relation, float from, float to, boolean includeLower, boolean includeUpper) {
        float[] lower = new float[] {includeLower ? from : Math.nextUp(from)};
        float[] upper = new float[] {includeUpper ? to : Math.nextDown(to)};
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = FloatRange.newWithinQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = FloatRange.newContainsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = FloatRange.newIntersectsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.FLOAT.dvRangeQuery(FIELDNAME, queryType, from, to,
                includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getDoubleRangeQuery(ShapeRelation relation, double from, double to, boolean includeLower,
                                      boolean includeUpper) {
        double[] lower = new double[] {includeLower ? from : Math.nextUp(from)};
        double[] upper = new double[] {includeUpper ? to : Math.nextDown(to)};
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = DoubleRange.newWithinQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery =  DoubleRange.newContainsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery =  DoubleRange.newIntersectsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.DOUBLE.dvRangeQuery(FIELDNAME, queryType, from, to,
                includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Query getInetAddressRangeQuery(ShapeRelation relation, InetAddress from, InetAddress to, boolean includeLower,
                                           boolean includeUpper) {
        InetAddress lower = includeLower ? from : InetAddressPoint.nextUp(from);
        InetAddress upper = includeUpper ? to : InetAddressPoint.nextDown(to);
        Query indexQuery;
        BinaryDocValuesRangeQuery.QueryType queryType;
        if (relation == ShapeRelation.WITHIN) {
            indexQuery = InetAddressRange.newWithinQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.WITHIN;
        } else if (relation == ShapeRelation.CONTAINS) {
            indexQuery = InetAddressRange.newContainsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.CONTAINS;
        } else {
            indexQuery = InetAddressRange.newIntersectsQuery(FIELDNAME, lower, upper);
            queryType = BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
        }
        Query dvQuery = RangeType.IP.dvRangeQuery(FIELDNAME, queryType, from, to,
                includeLower, includeUpper);
        return new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    private Object nextFrom() throws Exception {
        switch (type) {
            case INTEGER:
                return (int)(random().nextInt() * 0.5 - DISTANCE);
            case DATE:
                return DateTime.now();
            case LONG:
                return (long)(random().nextLong() * 0.5 - DISTANCE);
            case FLOAT:
                return (float)(random().nextFloat() * 0.5 - DISTANCE);
            case IP:
                return InetAddress.getByName("::ffff:c0a8:107");
            default:
                return random().nextDouble() * 0.5 - DISTANCE;
        }
    }

    private Object nextTo(Object from) throws Exception {
        switch (type) {
            case INTEGER:
                return (Integer)from + DISTANCE;
            case DATE:
                return DateTime.now().plusDays(DISTANCE);
            case LONG:
                return (Long)from + DISTANCE;
            case DOUBLE:
                return (Double)from + DISTANCE;
            case IP:
                return InetAddress.getByName("2001:db8::");
            default:
                return (Float)from + DISTANCE;
        }
    }

    public void testParseIp() {
        assertEquals(InetAddresses.forString("::1"), RangeFieldMapper.RangeType.IP.parse(InetAddresses.forString("::1"), randomBoolean()));
        assertEquals(InetAddresses.forString("::1"), RangeFieldMapper.RangeType.IP.parse("::1", randomBoolean()));
        assertEquals(InetAddresses.forString("::1"), RangeFieldMapper.RangeType.IP.parse(new BytesRef("::1"), randomBoolean()));
    }

    public void testTermQuery() throws Exception, IllegalArgumentException {
        // See https://github.com/elastic/elasticsearch/issues/25950
       RangeFieldMapper.RangeFieldType ft = this.rangeFieldType(type);

        Object value = nextFrom();
        ShapeRelation relation = ShapeRelation.INTERSECTS;
        boolean includeLower = true;
        boolean includeUpper = true;
        assertEquals(getExpectedRangeQuery(relation, value, value, includeLower, includeUpper),
            ft.termQuery(value, this.context()));
    }

    private QueryShardContext context() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);
        return new QueryShardContext(0,
            idxSettings,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            null,
            () -> nowInMillis,
            null);
    }
}
