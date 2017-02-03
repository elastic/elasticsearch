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
import org.apache.lucene.document.DoubleRangeField;
import org.apache.lucene.document.FloatRangeField;
import org.apache.lucene.document.IntRangeField;
import org.apache.lucene.document.LongRangeField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.RangeFieldMapper.RangeType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.IndexSettingsModule;
import org.joda.time.DateTime;
import org.junit.Before;

import java.util.Locale;

public class RangeFieldTypeTests extends FieldTypeTestCase {
    RangeType type;
    protected static String FIELDNAME = "field";
    protected static int DISTANCE = 10;
    private static long nowInMillis;

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
        return new RangeFieldMapper.RangeFieldType(type);
    }

    public void testRangeQuery() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAsciiOfLengthBetween(1, 10), indexSettings);
        QueryShardContext context = new QueryShardContext(0, idxSettings, null, null, null, null, null, xContentRegistry(),
                null, null, () -> nowInMillis);
        RangeFieldMapper.RangeFieldType ft = new RangeFieldMapper.RangeFieldType(type);
        ft.setName(FIELDNAME);
        ft.setIndexOptions(IndexOptions.DOCS);

        ShapeRelation relation = RandomPicks.randomFrom(random(), ShapeRelation.values());
        boolean includeLower = random().nextBoolean();
        boolean includeUpper = random().nextBoolean();
        Object from = nextFrom();
        Object to = nextTo(from);

        assertEquals(getExpectedRangeQuery(relation, from, to, includeLower, includeUpper),
            ft.rangeQuery(from, to, includeLower, includeUpper, relation, context));
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
            default:
                return getFloatRangeQuery(relation, (float)from, (float)to, includeLower, includeUpper);
        }
    }

    private Query getDateRangeQuery(ShapeRelation relation, DateTime from, DateTime to, boolean includeLower, boolean includeUpper) {
        return getLongRangeQuery(relation, from.getMillis(), to.getMillis(), includeLower, includeUpper);
    }

    private Query getIntRangeQuery(ShapeRelation relation, int from, int to, boolean includeLower, boolean includeUpper) {
        int[] lower = new int[] {from + (includeLower ? 0 : 1)};
        int[] upper = new int[] {to - (includeUpper ? 0 : 1)};
        if (relation == ShapeRelation.WITHIN) {
            return IntRangeField.newWithinQuery(FIELDNAME, lower, upper);
        } else if (relation == ShapeRelation.CONTAINS) {
            return IntRangeField.newContainsQuery(FIELDNAME, lower, upper);
        }
        return IntRangeField.newIntersectsQuery(FIELDNAME, lower, upper);
    }

    private Query getLongRangeQuery(ShapeRelation relation, long from, long to, boolean includeLower, boolean includeUpper) {
        long[] lower = new long[] {from + (includeLower ? 0 : 1)};
        long[] upper = new long[] {to - (includeUpper ? 0 : 1)};
        if (relation == ShapeRelation.WITHIN) {
            return LongRangeField.newWithinQuery(FIELDNAME, lower, upper);
        } else if (relation == ShapeRelation.CONTAINS) {
            return LongRangeField.newContainsQuery(FIELDNAME, lower, upper);
        }
        return LongRangeField.newIntersectsQuery(FIELDNAME, lower, upper);
    }

    private Query getFloatRangeQuery(ShapeRelation relation, float from, float to, boolean includeLower, boolean includeUpper) {
        float[] lower = new float[] {includeLower ? from : Math.nextUp(from)};
        float[] upper = new float[] {includeUpper ? to : Math.nextDown(to)};
        if (relation == ShapeRelation.WITHIN) {
            return FloatRangeField.newWithinQuery(FIELDNAME, lower, upper);
        } else if (relation == ShapeRelation.CONTAINS) {
            return FloatRangeField.newContainsQuery(FIELDNAME, lower, upper);
        }
        return FloatRangeField.newIntersectsQuery(FIELDNAME, lower, upper);
    }

    private Query getDoubleRangeQuery(ShapeRelation relation, double from, double to, boolean includeLower, boolean includeUpper) {
        double[] lower = new double[] {includeLower ? from : Math.nextUp(from)};
        double[] upper = new double[] {includeUpper ? to : Math.nextDown(to)};
        if (relation == ShapeRelation.WITHIN) {
            return DoubleRangeField.newWithinQuery(FIELDNAME, lower, upper);
        } else if (relation == ShapeRelation.CONTAINS) {
            return DoubleRangeField.newContainsQuery(FIELDNAME, lower, upper);
        }
        return DoubleRangeField.newIntersectsQuery(FIELDNAME, lower, upper);
    }

    private Object nextFrom() {
        switch (type) {
            case INTEGER:
                return (int)(random().nextInt() * 0.5 - DISTANCE);
            case DATE:
                return DateTime.now();
            case LONG:
                return (long)(random().nextLong() * 0.5 - DISTANCE);
            case FLOAT:
                return (float)(random().nextFloat() * 0.5 - DISTANCE);
            default:
                return random().nextDouble() * 0.5 - DISTANCE;
        }
    }

    private Object nextTo(Object from) {
        switch (type) {
            case INTEGER:
                return (Integer)from + DISTANCE;
            case DATE:
                return DateTime.now().plusDays(DISTANCE);
            case LONG:
                return (Long)from + DISTANCE;
            case DOUBLE:
                return (Double)from + DISTANCE;
            default:
                return (Float)from + DISTANCE;
        }
    }
}
