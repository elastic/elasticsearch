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

import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.queries.BinaryDocValuesRangeQuery;
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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper.RangeFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.IndexSettingsModule;
import org.joda.time.DateTime;
import org.junit.Before;

import java.net.InetAddress;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class RangeFieldTypeTests extends FieldTypeTestCase<RangeFieldType> {
    RangeType type;
    protected static String FIELDNAME = "field";
    protected static int DISTANCE = 10;
    private static long nowInMillis;

    @Before
    public void setupProperties() {
        type = randomFrom(RangeType.values());
        nowInMillis = randomNonNegativeLong();
        if (type == RangeType.DATE) {
            addModifier(t -> {
                RangeFieldType other = t.clone();
                if (other.dateTimeFormatter == DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER) {
                    other.setDateTimeFormatter(DateFormatter.forPattern("epoch_millis"));
                } else {
                    other.setDateTimeFormatter(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);
                }
                return other;
            });
        }
    }

    @Override
    protected RangeFieldType createDefaultFieldType() {
        return new RangeFieldType(type);
    }

    public void testRangeQuery() throws Exception {
        QueryShardContext context = createContext();
        RangeFieldType ft = new RangeFieldType(type);
        ft.setName(FIELDNAME);
        ft.setIndexOptions(IndexOptions.DOCS);

        ShapeRelation relation = randomFrom(ShapeRelation.values());
        boolean includeLower = randomBoolean();
        boolean includeUpper = randomBoolean();
        Object from = nextFrom();
        Object to = nextTo(from);
        if (includeLower == false && includeUpper == false) {
            // need to increase once more, otherwise interval is empty because edge values are exclusive
            to = nextTo(to);
        }

        assertEquals(getExpectedRangeQuery(relation, from, to, includeLower, includeUpper),
            ft.rangeQuery(from, to, includeLower, includeUpper, relation, null, null, context));
    }

    /**
     * test the queries are correct if from/to are adjacent and the range is exclusive of those values
     */
    public void testRangeQueryIntersectsAdjacentValues() throws Exception {
        QueryShardContext context = createContext();
        ShapeRelation relation = randomFrom(ShapeRelation.values());
        RangeFieldType ft = new RangeFieldType(type);
        ft.setName(FIELDNAME);
        ft.setIndexOptions(IndexOptions.DOCS);

        Object from = null;
        Object to = null;
        switch (type) {
            case LONG: {
                long fromValue = randomLong();
                from = fromValue;
                to = fromValue + 1;
                break;
            }
            case DATE: {
                long fromValue = randomInt();
                from = new DateTime(fromValue);
                to = new DateTime(fromValue + 1);
                break;
            }
            case INTEGER: {
                int fromValue = randomInt();
                from = fromValue;
                to = fromValue + 1;
                break;
            }
            case DOUBLE: {
                double fromValue = randomDoubleBetween(0, 100, true);
                from = fromValue;
                to = Math.nextUp(fromValue);
                break;
            }
            case FLOAT: {
                float fromValue = randomFloat();
                from = fromValue;
                to = Math.nextUp(fromValue);
                break;
            }
            case IP: {
                byte[] ipv4 = new byte[4];
                random().nextBytes(ipv4);
                InetAddress fromValue = InetAddress.getByAddress(ipv4);
                from = fromValue;
                to = InetAddressPoint.nextUp(fromValue);
                break;
            }
            default:
                from = nextFrom();
                to = nextTo(from);
        }
        Query rangeQuery = ft.rangeQuery(from, to, false, false, relation, null, null, context);
            assertThat(rangeQuery, instanceOf(IndexOrDocValuesQuery.class));
            assertThat(((IndexOrDocValuesQuery) rangeQuery).getIndexQuery(), instanceOf(MatchNoDocsQuery.class));
    }

    /**
     * check that we catch cases where the user specifies larger "from" than "to" value, not counting the include upper/lower settings
     */
    public void testFromLargerToErrors() throws Exception {
        QueryShardContext context = createContext();
        RangeFieldType ft = new RangeFieldType(type);
        ft.setName(FIELDNAME);
        ft.setIndexOptions(IndexOptions.DOCS);

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
                from = new DateTime(fromValue);
                to = new DateTime(fromValue - 1);
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
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () ->   ft.rangeQuery(from, to, true, true, relation, null, null, context));
        assertTrue(ex.getMessage().contains("Range query `from` value"));
        assertTrue(ex.getMessage().contains("is greater than `to` value"));
    }

    private QueryShardContext createContext() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);
        return new QueryShardContext(0, idxSettings, BigArrays.NON_RECYCLING_INSTANCE, null, null, null, null, null,
            xContentRegistry(), writableRegistry(), null, null, () -> nowInMillis, null, null, () -> true, null);
    }

    public void testDateRangeQueryUsingMappingFormat() {
        QueryShardContext context = createContext();
        RangeFieldType fieldType = new RangeFieldType(RangeType.DATE);
        fieldType.setName(FIELDNAME);
        fieldType.setIndexOptions(IndexOptions.DOCS);
        fieldType.setHasDocValues(false);
        // don't use DISJOINT here because it doesn't work on date fields which we want to compare bounds with
        ShapeRelation relation = randomValueOtherThan(ShapeRelation.DISJOINT,() -> randomFrom(ShapeRelation.values()));

        // dates will break the default format, month/day of month is turned around in the format
        final String from = "2016-15-06T15:29:50+08:00";
        final String to = "2016-16-06T15:29:50+08:00";

        ElasticsearchParseException ex = expectThrows(ElasticsearchParseException.class,
            () -> fieldType.rangeQuery(from, to, true, true, relation, null, null, context));
        assertThat(ex.getMessage(),
            containsString("failed to parse date field [2016-15-06T15:29:50+08:00] with format [strict_date_optional_time||epoch_millis]")
        );

        // setting mapping format which is compatible with those dates
        final DateFormatter formatter = DateFormatter.forPattern("yyyy-dd-MM'T'HH:mm:ssZZZZZ");
        assertEquals(1465975790000L, formatter.parseMillis(from));
        assertEquals(1466062190000L, formatter.parseMillis(to));

        fieldType.setDateTimeFormatter(formatter);
        final Query query = fieldType.rangeQuery(from, to, true, true, relation, null, null, context);
        assertEquals("field:<ranges:[1465975790000 : 1466062190999]>", query.toString());

        // compare lower and upper bounds with what we would get on a `date` field
        DateFieldType dateFieldType = new DateFieldType();
        dateFieldType.setName(FIELDNAME);
        dateFieldType.setDateTimeFormatter(formatter);
        final Query queryOnDateField = dateFieldType.rangeQuery(from, to, true, true, relation, null, null, context);
        assertEquals("field:[1465975790000 TO 1466062190999]", queryOnDateField.toString());
    }

    /**
     * We would like to ensure lower and upper bounds are consistent between queries on a `date` and a`date_range`
     * field, so we randomize a few cases and compare the generated queries here
     */
    public void testDateVsDateRangeBounds() {
        QueryShardContext context = createContext();
        RangeFieldType fieldType = new RangeFieldType(RangeType.DATE);
        fieldType.setName(FIELDNAME);
        fieldType.setIndexOptions(IndexOptions.DOCS);
        fieldType.setHasDocValues(false);

        // date formatter that truncates seconds, so we get some rounding behavior
        final DateFormatter formatter = DateFormatter.forPattern("yyyy-dd-MM'T'HH:mm");
        long lower = randomLongBetween(formatter.parseMillis("2000-01-01T00:00"), formatter.parseMillis("2010-01-01T00:00"));
        long upper = randomLongBetween(formatter.parseMillis("2011-01-01T00:00"), formatter.parseMillis("2020-01-01T00:00"));

        fieldType.setDateTimeFormatter(formatter);
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
        final Query query = fieldType.rangeQuery(lowerAsString, upperAsString, includeLower, includeUpper, ShapeRelation.INTERSECTS, null,
                null, context);

        // get exact lower and upper bounds similar to what we would parse for `date` fields for same input strings
        DateFieldType dateFieldType = new DateFieldType();
        long lowerBoundLong = dateFieldType.parseToLong(lowerAsString, !includeLower, null, formatter.toDateMathParser(), () -> 0);
        if (includeLower == false) {
            ++lowerBoundLong;
        }
        long upperBoundLong = dateFieldType.parseToLong(upperAsString, includeUpper, null, formatter.toDateMathParser(), () -> 0);
        if (includeUpper == false) {
            --upperBoundLong;
        }

        // check that using this bounds we get similar query when constructing equivalent query on date_range field
        Query range = LongRange.newIntersectsQuery(FIELDNAME, new long[] { lowerBoundLong }, new long[] { upperBoundLong });
        assertEquals(range, query);
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
        assertEquals(InetAddresses.forString("::1"), RangeType.IP.parse(InetAddresses.forString("::1"), randomBoolean()));
        assertEquals(InetAddresses.forString("::1"), RangeType.IP.parse("::1", randomBoolean()));
        assertEquals(InetAddresses.forString("::1"), RangeType.IP.parse(new BytesRef("::1"), randomBoolean()));
    }

    public void testTermQuery() throws Exception {
        // See https://github.com/elastic/elasticsearch/issues/25950
        QueryShardContext context = createContext();
        RangeFieldType ft = new RangeFieldType(type);
        ft.setName(FIELDNAME);
        ft.setIndexOptions(IndexOptions.DOCS);

        Object value = nextFrom();
        ShapeRelation relation = ShapeRelation.INTERSECTS;
        boolean includeLower = true;
        boolean includeUpper = true;
        assertEquals(getExpectedRangeQuery(relation, value, value, includeLower, includeUpper),
            ft.termQuery(value, context));
    }
}
