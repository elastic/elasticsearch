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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;

public class DateFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new DateFieldMapper.DateFieldType();
    }

    private static long nowInMillis;

    @Before
    public void setupProperties() {
        setDummyNullValue(10);
        addModifier(new Modifier("format", false) {
            @Override
            public void modify(MappedFieldType ft) {
                ((DateFieldType) ft).setDateTimeFormatter(Joda.forPattern("basic_week_date", Locale.ROOT));
            }
        });
        addModifier(new Modifier("locale", false) {
            @Override
            public void modify(MappedFieldType ft) {
                ((DateFieldType) ft).setDateTimeFormatter(Joda.forPattern("date_optional_time", Locale.CANADA));
            }
        });
        nowInMillis = randomNonNegativeLong();
    }

    public void testIsFieldWithinQueryEmptyReader() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> nowInMillis);
        IndexReader reader = new MultiReader();
        DateFieldType ft = new DateFieldType();
        ft.setName("my_date");
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                randomBoolean(), randomBoolean(), null, null, context));
    }

    private void doTestIsFieldWithinQuery(DateFieldType ft, DirectoryReader reader,
            DateTimeZone zone, DateMathParser alternateFormat) throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> nowInMillis);
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-09", "2016-01-02",
                randomBoolean(), randomBoolean(), null, null, context));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2016-01-02", "2016-06-20",
                randomBoolean(), randomBoolean(), null, null, context));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2016-01-02", "2016-02-12",
                randomBoolean(), randomBoolean(), null, null, context));
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2014-01-02", "2015-02-12",
                randomBoolean(), randomBoolean(), null, null, context));
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2016-05-11", "2016-08-30",
                randomBoolean(), randomBoolean(), null, null, context));
        assertEquals(Relation.WITHIN, ft.isFieldWithinQuery(reader, "2015-09-25", "2016-05-29",
                randomBoolean(), randomBoolean(), null, null, context));
        assertEquals(Relation.WITHIN, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                true, true, null, null, context));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                false, false, null, null, context));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                false, true, null, null, context));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                true, false, null, null, context));
    }

    public void testIsFieldWithinQuery() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        long instant1 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime("2015-10-12").getMillis();
        long instant2 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime("2016-04-03").getMillis();
        Document doc = new Document();
        LongPoint field = new LongPoint("my_date", instant1);
        doc.add(field);
        w.addDocument(doc);
        field.setLongValue(instant2);
        w.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(w);
        DateFieldType ft = new DateFieldType();
        ft.setName("my_date");
        DateMathParser alternateFormat = new DateMathParser(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);
        doTestIsFieldWithinQuery(ft, reader, null, null);
        doTestIsFieldWithinQuery(ft, reader, null, alternateFormat);
        doTestIsFieldWithinQuery(ft, reader, DateTimeZone.UTC, null);
        doTestIsFieldWithinQuery(ft, reader, DateTimeZone.UTC, alternateFormat);

        // Fields with no value indexed.
        DateFieldType ft2 = new DateFieldType();
        ft2.setName("my_date2");

        QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> nowInMillis);
        assertEquals(Relation.DISJOINT, ft2.isFieldWithinQuery(reader, "2015-10-09", "2016-01-02", false, false, null, null, context));
        IOUtils.close(reader, w, dir);
    }

    public void testValueFormat() {
        MappedFieldType ft = createDefaultFieldType();
        long instant = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime("2015-10-12T14:10:55").getMillis();
        assertEquals("2015-10-12T14:10:55.000Z",
                ft.docValueFormat(null, DateTimeZone.UTC).format(instant));
        assertEquals("2015-10-12T15:10:55.000+01:00",
                ft.docValueFormat(null, DateTimeZone.forOffsetHours(1)).format(instant));
        assertEquals("2015",
                createDefaultFieldType().docValueFormat("YYYY", DateTimeZone.UTC).format(instant));
        assertEquals(instant,
                ft.docValueFormat(null, DateTimeZone.UTC).parseLong("2015-10-12T14:10:55", false, null));
        assertEquals(instant + 999,
                ft.docValueFormat(null, DateTimeZone.UTC).parseLong("2015-10-12T14:10:55", true, null));
        assertEquals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime("2015-10-13").getMillis() - 1,
                ft.docValueFormat(null, DateTimeZone.UTC).parseLong("2015-10-12||/d", true, null));
    }

    public void testValueForSearch() {
        MappedFieldType ft = createDefaultFieldType();
        String date = "2015-10-12T12:09:55.000Z";
        long instant = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime(date).getMillis();
        assertEquals(date, ft.valueForDisplay(instant));
    }

    public void testTermQuery() {
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).build();
        QueryShardContext context = new QueryShardContext(0,
                new IndexSettings(IndexMetaData.builder("foo").settings(indexSettings).build(),
                        indexSettings),
                null, null, null, null, null, xContentRegistry(), writableRegistry(), null, null, () -> nowInMillis, null);
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        String date = "2015-10-12T14:10:55";
        long instant = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime(date).getMillis();
        ft.setIndexOptions(IndexOptions.DOCS);
        Query expected = new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery("field", instant, instant + 999),
                SortedNumericDocValuesField.newSlowRangeQuery("field", instant, instant + 999));
        assertEquals(expected, ft.termQuery(date, context));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.termQuery(date, context));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRangeQuery() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).build();
        QueryShardContext context = new QueryShardContext(0,
                new IndexSettings(IndexMetaData.builder("foo").settings(indexSettings).build(), indexSettings),
                null, null, null, null, null, xContentRegistry(), writableRegistry(), null, null, () -> nowInMillis, null);
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        String date1 = "2015-10-12T14:10:55";
        String date2 = "2016-04-28T11:33:52";
        long instant1 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime(date1).getMillis();
        long instant2 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime(date2).getMillis() + 999;
        ft.setIndexOptions(IndexOptions.DOCS);
        Query expected = new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery("field", instant1, instant2),
                SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2));
        assertEquals(expected,
                ft.rangeQuery(date1, date2, true, true, null, null, null, context).rewrite(new MultiReader()));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.rangeQuery(date1, date2, true, true, null, null, null, context));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }
}
