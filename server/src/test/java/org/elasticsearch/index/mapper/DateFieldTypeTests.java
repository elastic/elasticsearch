/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.query.DateRangeIncludingNowQuery;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.script.field.DateNanosDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

public class DateFieldTypeTests extends FieldTypeTestCase {

    private static final long nowInMillis = 0;

    public void testIsFieldWithinRangeEmptyReader() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(parserConfig(), null, () -> nowInMillis);
        IndexReader reader = new MultiReader();
        DateFieldType ft = new DateFieldType("my_date");
        assertEquals(
            Relation.DISJOINT,
            ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03", randomBoolean(), randomBoolean(), null, null, context)
        );
    }

    public void testIsFieldWithinRangeOnlyDocValues() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(parserConfig(), null, () -> nowInMillis);
        IndexReader reader = new MultiReader();
        DateFieldType ft = new DateFieldType("my_date", false);
        // in case of only doc-values, we can't establish disjointness
        assertEquals(
            Relation.INTERSECTS,
            ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03", randomBoolean(), randomBoolean(), null, null, context)
        );
    }

    public void testIsFieldWithinQueryDateMillis() throws IOException {
        DateFieldType ft = new DateFieldType("my_date");
        isFieldWithinRangeTestCase(ft);
    }

    public void testIsFieldWithinQueryDateNanos() throws IOException {
        DateFieldType ft = new DateFieldType("my_date", Resolution.NANOSECONDS);
        isFieldWithinRangeTestCase(ft);
    }

    public void testIsFieldWithinQueryDateMillisDocValueSkipper() throws IOException {
        DateFieldType ft = new DateFieldType(
            "my_date",
            false,
            false,
            false,
            true,
            true,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
        isFieldWithinRangeTestCase(ft);
    }

    public void testIsFieldWithinQueryDateNanosDocValueSkipper() throws IOException {
        DateFieldType ft = new DateFieldType(
            "my_date",
            false,
            false,
            false,
            true,
            true,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.NANOSECONDS,
            null,
            null,
            Collections.emptyMap()
        );
        isFieldWithinRangeTestCase(ft);
    }

    public void isFieldWithinRangeTestCase(DateFieldType ft) throws IOException {

        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        LuceneDocument doc = new LuceneDocument();
        Field field;
        if (ft.hasDocValuesSkipper()) {
            field = SortedNumericDocValuesField.indexedField("my_date", ft.parse("2015-10-12"));
        } else {
            field = new LongPoint("my_date", ft.parse("2015-10-12"));
        }
        doc.add(field);
        w.addDocument(doc);
        field.setLongValue(ft.parse("2016-04-03"));
        w.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(w);

        DateMathParser alternateFormat = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser();
        doTestIsFieldWithinQuery(ft, reader, null, null);
        doTestIsFieldWithinQuery(ft, reader, null, alternateFormat);
        doTestIsFieldWithinQuery(ft, reader, ZoneOffset.UTC, null);
        doTestIsFieldWithinQuery(ft, reader, ZoneOffset.UTC, alternateFormat);

        QueryRewriteContext context = new QueryRewriteContext(parserConfig(), null, () -> nowInMillis);

        // Fields with no value indexed.
        DateFieldType ft2 = new DateFieldType("my_date2");

        assertEquals(Relation.DISJOINT, ft2.isFieldWithinQuery(reader, "2015-10-09", "2016-01-02", false, false, null, null, context));

        IOUtils.close(reader, w, dir);
    }

    private void doTestIsFieldWithinQuery(DateFieldType ft, DirectoryReader reader, ZoneId zone, DateMathParser alternateFormat)
        throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(parserConfig(), null, () -> nowInMillis);
        assertEquals(
            Relation.INTERSECTS,
            ft.isFieldWithinQuery(reader, "2015-10-09", "2016-01-02", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertEquals(
            Relation.INTERSECTS,
            ft.isFieldWithinQuery(reader, "2016-01-02", "2016-06-20", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertEquals(
            Relation.INTERSECTS,
            ft.isFieldWithinQuery(reader, "2016-01-02", "2016-02-12", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertEquals(
            Relation.DISJOINT,
            ft.isFieldWithinQuery(reader, "2014-01-02", "2015-02-12", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertEquals(
            Relation.DISJOINT,
            ft.isFieldWithinQuery(reader, "2016-05-11", "2016-08-30", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertEquals(
            Relation.WITHIN,
            ft.isFieldWithinQuery(reader, "2015-09-25", "2016-05-29", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertEquals(Relation.WITHIN, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03", true, true, zone, null, context));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03", false, false, zone, null, context));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03", false, true, zone, null, context));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03", true, false, zone, null, context));
        // Bad dates
        assertThrows(
            ElasticsearchParseException.class,
            () -> ft.isFieldWithinQuery(reader, "2015-00-01", "2016-04-03", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertThrows(
            ElasticsearchParseException.class,
            () -> ft.isFieldWithinQuery(reader, "2015-01-01", "2016-04-00", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertThrows(
            ElasticsearchParseException.class,
            () -> ft.isFieldWithinQuery(reader, "2015-22-01", "2016-04-00", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertThrows(
            ElasticsearchParseException.class,
            () -> ft.isFieldWithinQuery(reader, "2015-01-01", "2016-04-45", randomBoolean(), randomBoolean(), zone, null, context)
        );
        assertThrows(
            ElasticsearchParseException.class,
            () -> ft.isFieldWithinQuery(reader, "2015-01-01", "2016-04-01T25:00:00", randomBoolean(), randomBoolean(), zone, null, context)
        );
        if (ft.resolution().equals(Resolution.NANOSECONDS)) {
            assertThrows(
                IllegalArgumentException.class,
                () -> ft.isFieldWithinQuery(reader, "-2016-04-01", "2016-04-01", randomBoolean(), randomBoolean(), zone, null, context)
            );
            assertThrows(
                IllegalArgumentException.class,
                () -> ft.isFieldWithinQuery(
                    reader,
                    "9223372036854775807",
                    "2016-04-01",
                    randomBoolean(),
                    randomBoolean(),
                    zone,
                    null,
                    context
                )
            );
        }
    }

    public void testValueFormat() {
        MappedFieldType ft = new DateFieldType("field");
        long instant = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse("2015-10-12T14:10:55"))
            .toInstant()
            .toEpochMilli();

        assertEquals("2015-10-12T14:10:55.000Z", ft.docValueFormat(null, ZoneOffset.UTC).format(instant));
        assertEquals("2015-10-12T15:10:55.000+01:00", ft.docValueFormat(null, ZoneOffset.ofHours(1)).format(instant));
        assertEquals("2015", new DateFieldType("field").docValueFormat("YYYY", ZoneOffset.UTC).format(instant));
        assertEquals(instant, ft.docValueFormat(null, ZoneOffset.UTC).parseLong("2015-10-12T14:10:55", false, null));
        assertEquals(instant + 999, ft.docValueFormat(null, ZoneOffset.UTC).parseLong("2015-10-12T14:10:55", true, null));
        long i = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse("2015-10-13")).toInstant().toEpochMilli();
        assertEquals(i - 1, ft.docValueFormat(null, ZoneOffset.UTC).parseLong("2015-10-12||/d", true, null));
    }

    public void testValueForSearch() {
        MappedFieldType ft = new DateFieldType("field");
        String date = "2015-10-12T12:09:55.000Z";
        long instant = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(date);
        assertEquals(date, ft.valueForDisplay(instant));
    }

    public void testTermQuery() {
        Settings indexSettings = indexSettings(IndexVersion.current(), 1, 1).build();
        SearchExecutionContext context = SearchExecutionContextHelper.createSimple(
            new IndexSettings(IndexMetadata.builder("foo").settings(indexSettings).build(), indexSettings),
            parserConfig(),
            writableRegistry()
        );
        MappedFieldType ft = new DateFieldType("field");
        String date = "2015-10-12T14:10:55";
        long instant = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date)).toInstant().toEpochMilli();
        Query expected = new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery("field", instant, instant + 999),
            SortedNumericDocValuesField.newSlowRangeQuery("field", instant, instant + 999)
        );
        assertEquals(expected, ft.termQuery(date, context));

        ft = new DateFieldType("field", false);
        expected = SortedNumericDocValuesField.newSlowRangeQuery("field", instant, instant + 999);
        assertEquals(expected, ft.termQuery(date, context));

        MappedFieldType unsearchable = new DateFieldType(
            "field",
            false,
            false,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery(date, context));
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testRangeQuery() throws IOException {
        Settings indexSettings = indexSettings(IndexVersion.current(), 1, 1).build();
        SearchExecutionContext context = new SearchExecutionContext(
            0,
            0,
            new IndexSettings(IndexMetadata.builder("foo").settings(indexSettings).build(), indexSettings),
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
            Collections.emptyMap(),
            MapperMetrics.NOOP
        );
        MappedFieldType ft = new DateFieldType("field");
        String date1 = "2015-10-12T14:10:55";
        String date2 = "2016-04-28T11:33:52";
        long instant1 = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date1)).toInstant().toEpochMilli();
        long instant2 = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date2)).toInstant().toEpochMilli() + 999;
        Query expected = new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery("field", instant1, instant2),
            SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2)
        );
        assertEquals(expected, ft.rangeQuery(date1, date2, true, true, null, null, null, context).rewrite(newSearcher(new MultiReader())));

        MappedFieldType ft2 = new DateFieldType("field", false);
        Query expected2 = SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2);
        assertEquals(
            expected2,
            ft2.rangeQuery(date1, date2, true, true, null, null, null, context).rewrite(newSearcher(new MultiReader()))
        );

        instant1 = nowInMillis;
        instant2 = instant1 + 100;
        expected = new DateRangeIncludingNowQuery(
            new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery("field", instant1, instant2),
                SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2)
            )
        );
        assertEquals(expected, ft.rangeQuery("now", instant2, true, true, null, null, null, context));

        expected2 = new DateRangeIncludingNowQuery(SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2));
        assertEquals(expected2, ft2.rangeQuery("now", instant2, true, true, null, null, null, context));

        MappedFieldType unsearchable = new DateFieldType(
            "field",
            false,
            false,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.rangeQuery(date1, date2, true, true, null, null, null, context)
        );
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testRangeQueryWithIndexSort() {
        Settings settings = indexSettings(IndexVersion.current(), 1, 1).put("index.sort.field", "field").build();

        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);

        SearchExecutionContext context = SearchExecutionContextHelper.createSimple(indexSettings, parserConfig(), writableRegistry());

        MappedFieldType ft = new DateFieldType("field");
        String date1 = "2015-10-12T14:10:55";
        String date2 = "2016-04-28T11:33:52";
        long instant1 = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date1)).toInstant().toEpochMilli();
        long instant2 = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date2)).toInstant().toEpochMilli() + 999;

        Query pointQuery = LongPoint.newRangeQuery("field", instant1, instant2);
        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2);
        Query expected = new IndexSortSortedNumericDocValuesRangeQuery(
            "field",
            instant1,
            instant2,
            new IndexOrDocValuesQuery(pointQuery, dvQuery)
        );
        assertEquals(expected, ft.rangeQuery(date1, date2, true, true, null, null, null, context));

        ft = new DateFieldType("field", false);
        expected = new IndexSortSortedNumericDocValuesRangeQuery("field", instant1, instant2, dvQuery);
        assertEquals(expected, ft.rangeQuery(date1, date2, true, true, null, null, null, context));
    }

    public void testDateNanoDocValues() throws IOException {
        // Create an index with some docValues
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        LuceneDocument doc = new LuceneDocument();
        NumericDocValuesField docValuesField = new NumericDocValuesField("my_date", 1444608000000L);
        doc.add(docValuesField);
        w.addDocument(doc);
        docValuesField.setLongValue(1459641600000L);
        w.addDocument(doc);
        // Create the doc values reader
        SortedNumericIndexFieldData fieldData = new SortedNumericIndexFieldData(
            "my_date",
            IndexNumericFieldData.NumericType.DATE_NANOSECONDS,
            CoreValuesSourceType.DATE,
            DateNanosDocValuesField::new,
            false
        );
        // Read index and check the doc values
        DirectoryReader reader = DirectoryReader.open(w);
        assertTrue(reader.leaves().size() > 0);
        LeafNumericFieldData a = fieldData.load(reader.leaves().get(0).reader().getContext());
        SortedNumericDocValues docValues = a.getLongValues();
        assertEquals(0, docValues.nextDoc());
        assertEquals(1, docValues.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.nextDoc());
        reader.close();
        w.close();
        dir.close();
    }

    private static DateFieldType fieldType(Resolution resolution, String format, String nullValue) {
        DateFormatter formatter = DateFormatter.forPattern(format);
        return new DateFieldType("field", true, false, true, formatter, resolution, nullValue, null, Collections.emptyMap());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType fieldType = new DateFieldType("field", Resolution.MILLISECONDS);
        String date = "2020-05-15T21:33:02.000Z";
        assertEquals(List.of(date), fetchSourceValue(fieldType, date));
        assertEquals(List.of(date), fetchSourceValue(fieldType, 1589578382000L));

        MappedFieldType fieldWithFormat = fieldType(Resolution.MILLISECONDS, "yyyy/MM/dd||epoch_millis", null);
        String dateInFormat = "1990/12/29";
        assertEquals(List.of(dateInFormat), fetchSourceValue(fieldWithFormat, dateInFormat));
        assertEquals(List.of(dateInFormat), fetchSourceValue(fieldWithFormat, 662428800000L));

        MappedFieldType millis = fieldType(Resolution.MILLISECONDS, "epoch_millis", null);
        String dateInMillis = "662428800000";
        assertEquals(List.of(dateInMillis), fetchSourceValue(millis, dateInMillis));
        assertEquals(List.of(dateInMillis), fetchSourceValue(millis, 662428800000L));

        String nullValueDate = "2020-05-15T21:33:02.000Z";
        MappedFieldType nullFieldType = fieldType(Resolution.MILLISECONDS, "strict_date_time", nullValueDate);
        assertEquals(List.of(nullValueDate), fetchSourceValue(nullFieldType, null));
    }

    public void testParseSourceValueWithFormat() throws IOException {
        MappedFieldType mapper = fieldType(Resolution.NANOSECONDS, "strict_date_time", "1970-12-29T00:00:00.000Z");
        String date = "1990-12-29T00:00:00.000Z";
        assertEquals(List.of("1990/12/29"), fetchSourceValue(mapper, date, "yyyy/MM/dd"));
        assertEquals(List.of("662428800000"), fetchSourceValue(mapper, date, "epoch_millis"));
        assertEquals(List.of("1970/12/29"), fetchSourceValue(mapper, null, "yyyy/MM/dd"));
    }

    public void testParseSourceValueNanos() throws IOException {
        MappedFieldType mapper = fieldType(Resolution.NANOSECONDS, "strict_date_time||epoch_millis", null);
        String date = "2020-05-15T21:33:02.123456789Z";
        assertEquals(List.of("2020-05-15T21:33:02.123456789Z"), fetchSourceValue(mapper, date));
        assertEquals(List.of("2020-05-15T21:33:02.123Z"), fetchSourceValue(mapper, 1589578382123L));

        String nullValueDate = "2020-05-15T21:33:02.123456789Z";
        MappedFieldType nullValueMapper = fieldType(Resolution.NANOSECONDS, "strict_date_time||epoch_millis", nullValueDate);
        assertEquals(List.of(nullValueDate), fetchSourceValue(nullValueMapper, null));
    }
}
