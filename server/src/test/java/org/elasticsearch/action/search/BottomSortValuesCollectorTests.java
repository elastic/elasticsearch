/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;

import java.time.ZoneId;
import java.util.Arrays;

import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BottomSortValuesCollectorTests extends ESTestCase {
    public void testWithStrings() {
        for (boolean reverse : new boolean[] { true, false }) {
            SortField[] sortFields = new SortField[] { new SortField("foo", SortField.Type.STRING_VAL, reverse) };
            DocValueFormat[] sortFormats = new DocValueFormat[] { DocValueFormat.RAW };
            BottomSortValuesCollector collector = new BottomSortValuesCollector(3, sortFields);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newBytesArray("foo", "goo", "hoo")), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newBytesArray("bar", "car", "zar")), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newBytesArray()), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newBytesArray("tar", "zar", "zzz")), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newBytesArray(null, null, "zzz")), sortFormats);
            assertThat(collector.getTotalHits(), equalTo(350L));
            assertNotNull(collector.getBottomSortValues());
            assertThat(collector.getBottomSortValues().getSortValueFormats().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getSortValueFormats()[0], instanceOf(DocValueFormat.RAW.getClass()));
            assertThat(collector.getBottomSortValues().getRawSortValues().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getFormattedSortValues().length, equalTo(1));
            if (reverse) {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(new BytesRef("tar")));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo("tar"));
            } else {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(new BytesRef("hoo")));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo("hoo"));
            }
        }
    }

    public void testWithLongs() {
        for (boolean reverse : new boolean[] { true, false }) {
            SortField[] sortFields = new SortField[] { new SortField("foo", SortField.Type.LONG, reverse) };
            DocValueFormat[] sortFormats = new DocValueFormat[]{ DocValueFormat.RAW };
            BottomSortValuesCollector collector = new BottomSortValuesCollector(3, sortFields);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newLongArray(5L, 10L, 15L)), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newLongArray(25L, 350L, 3500L)), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newLongArray(1L, 2L, 3L)), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newLongArray()), sortFormats);
            // ignore bottom if we have less top docs than the requested size
            collector.consumeTopDocs(createTopDocs(sortFields[0], 1,
                newLongArray(-100L)), sortFormats);
            assertNotNull(collector.getBottomSortValues());
            assertThat(collector.getTotalHits(), equalTo(301L));
            assertThat(collector.getBottomSortValues().getSortValueFormats().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getSortValueFormats()[0], instanceOf(DocValueFormat.RAW.getClass()));
            assertThat(collector.getBottomSortValues().getRawSortValues().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getFormattedSortValues().length, equalTo(1));
            if (reverse) {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(25L));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo(25L));
            } else {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(3L));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo(3L));
            }
        }
    }

    public void testWithDoubles() {
        for (boolean reverse : new boolean[] { true, false }) {
            SortField[] sortFields = new SortField[]{ new SortField("foo", SortField.Type.LONG, reverse) };
            DocValueFormat[] sortFormats = new DocValueFormat[] { DocValueFormat.RAW };
            BottomSortValuesCollector collector = new BottomSortValuesCollector(3, sortFields);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newDoubleArray(500d, 5000d, 6755d)), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newDoubleArray(0.1d, 1.5d, 3.5d)), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newDoubleArray()), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newDoubleArray(100d, 101d, 102d)), sortFormats);
            // ignore bottom if we have less top docs than the requested size
            collector.consumeTopDocs(createTopDocs(sortFields[0], 2,
                newDoubleArray(0d, 1d)), sortFormats);
            assertThat(collector.getTotalHits(), equalTo(302L));
            assertNotNull(collector.getBottomSortValues());
            assertThat(collector.getBottomSortValues().getSortValueFormats().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getSortValueFormats()[0], instanceOf(DocValueFormat.RAW.getClass()));
            assertThat(collector.getBottomSortValues().getRawSortValues().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getFormattedSortValues().length, equalTo(1));
            if (reverse) {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(500d));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo(500d));
            } else {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(3.5d));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo(3.5d));
            }
        }
    }

    public void testWithDates() {
        for (boolean reverse : new boolean[] { true, false }) {
            SortField[] sortFields = new SortField[]{ new SortField("foo", SortField.Type.LONG, reverse) };
            DocValueFormat[] sortFormats = new DocValueFormat[] {
                new DocValueFormat.DateTime(DEFAULT_DATE_TIME_FORMATTER, ZoneId.of("UTC"), DateFieldMapper.Resolution.MILLISECONDS)};
            BottomSortValuesCollector collector = new BottomSortValuesCollector(3, sortFields);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newDateArray("2017-06-01T12:18:20Z", "2018-04-03T15:10:27Z", "2013-06-01T13:10:20Z")), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newDateArray("2018-05-21T08:10:10Z", "2015-02-08T15:12:34Z", "2015-01-01T13:10:30Z")), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newDateArray()), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newDateArray("2019-12-30T07:34:20Z", "2017-03-01T12:10:30Z", "2015-07-09T14:00:30Z")), sortFormats);
            assertThat(collector.getTotalHits(), equalTo(300L));
            assertNotNull(collector.getBottomSortValues());
            assertThat(collector.getBottomSortValues().getSortValueFormats().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getSortValueFormats()[0], instanceOf(DocValueFormat.DateTime.class));
            assertThat(collector.getBottomSortValues().getRawSortValues().length, equalTo(1));
            if (reverse) {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(1436450430000L));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo("2015-07-09T14:00:30.000Z"));
            } else {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(1522768227000L));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo("2018-04-03T15:10:27.000Z"));
            }
        }
    }

    public void testWithDateNanos() {
        for (boolean reverse : new boolean[] { true, false }) {
            SortField[] sortFields = new SortField[]{ new SortField("foo", SortField.Type.LONG, reverse) };
            DocValueFormat[] sortFormats = new DocValueFormat[] {
                new DocValueFormat.DateTime(DEFAULT_DATE_TIME_FORMATTER, ZoneId.of("UTC"), DateFieldMapper.Resolution.NANOSECONDS)};
            BottomSortValuesCollector collector = new BottomSortValuesCollector(3, sortFields);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newDateNanoArray("2017-06-01T12:18:20Z", "2018-04-03T15:10:27Z", "2013-06-01T13:10:20Z")), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newDateNanoArray("2018-05-21T08:10:10Z", "2015-02-08T15:12:34Z", "2015-01-01T13:10:30Z")), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newDateNanoArray()), sortFormats);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 50,
                newDateNanoArray("2019-12-30T07:34:20Z", "2017-03-01T12:10:30Z", "2015-07-09T14:00:30Z")), sortFormats);
            assertThat(collector.getTotalHits(), equalTo(300L));
            assertNotNull(collector.getBottomSortValues());
            assertThat(collector.getBottomSortValues().getSortValueFormats().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getSortValueFormats()[0], instanceOf(DocValueFormat.DateTime.class));
            assertThat(collector.getBottomSortValues().getRawSortValues().length, equalTo(1));
            if (reverse) {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(1436450430000000000L));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo("2015-07-09T14:00:30.000Z"));
            } else {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(1522768227000000000L));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo("2018-04-03T15:10:27.000Z"));
            }
        }
    }

    public void testWithMixedTypes() {
        for (boolean reverse : new boolean[] { true, false }) {
            SortField[] sortFields = new SortField[] { new SortField("foo", SortField.Type.LONG, reverse) };
            SortField[] otherSortFields = new SortField[] { new SortField("foo", SortField.Type.STRING_VAL, reverse) };
            DocValueFormat[] sortFormats = new DocValueFormat[] { DocValueFormat.RAW };
            BottomSortValuesCollector collector = new BottomSortValuesCollector(3, sortFields);
            collector.consumeTopDocs(createTopDocs(sortFields[0], 100,
                newLongArray(1000L, 100L, 10L)), sortFormats);
            collector.consumeTopDocs(createTopDocs(otherSortFields[0], 50,
                newBytesArray("foo", "bar", "zoo")), sortFormats);
            assertThat(collector.getTotalHits(), equalTo(150L));
            assertNotNull(collector.getBottomSortValues());
            assertThat(collector.getBottomSortValues().getSortValueFormats().length, equalTo(1));
            assertThat(collector.getBottomSortValues().getSortValueFormats()[0], instanceOf(DocValueFormat.RAW.getClass()));
            assertThat(collector.getBottomSortValues().getRawSortValues().length, equalTo(1));
            if (reverse) {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(10L));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo(10L));
            } else {
                assertThat(collector.getBottomSortValues().getRawSortValues()[0], equalTo(1000L));
                assertThat(collector.getBottomSortValues().getFormattedSortValues()[0], equalTo(1000L));
            }
        }
    }

    private Object[] newDoubleArray(Double... values) {
        return values;
    }

    private Object[] newLongArray(Long... values) {
        return values;
    }

    private Object[] newBytesArray(String... values) {
        BytesRef[] bytesRefs = new BytesRef[values.length];
        for (int i = 0; i < bytesRefs.length; i++) {
            bytesRefs[i] = values[i] == null ? null : new BytesRef(values[i]);
        }
        return bytesRefs;
    }

    private Object[] newDateArray(String... values) {
        Long[] longs = new Long[values.length];
        for (int i = 0; i < values.length; i++) {
            longs[i] = DEFAULT_DATE_TIME_FORMATTER.parseMillis(values[i]);
        }
        return longs;
    }

    private Object[] newDateNanoArray(String... values) {
        Long[] longs = new Long[values.length];
        for (int i = 0; i < values.length; i++) {
            longs[i] = DateUtils.toNanoSeconds(DEFAULT_DATE_TIME_FORMATTER.parseMillis(values[i]));
        }
        return longs;
    }

    private TopFieldDocs createTopDocs(SortField sortField, int totalHits, Object[] values) {
        FieldDoc[] fieldDocs = new FieldDoc[values.length];
        @SuppressWarnings("unchecked")
        FieldComparator<Object> cmp = (FieldComparator<Object>) sortField.getComparator(1, 0);
        for (int i = 0; i < values.length; i++) {
            fieldDocs[i] = new FieldDoc(i, Float.NaN, new Object[] { values[i] });
        }
        int reverseMul = sortField.getReverse() ? -1 : 1;
        Arrays.sort(fieldDocs, (o1, o2) -> reverseMul * cmp.compareValues(o1.fields[0], o2.fields[0]));
        return new TopFieldDocs(new TotalHits(totalHits, GREATER_THAN_OR_EQUAL_TO),
            fieldDocs, new SortField[] { sortField });
    }
}
