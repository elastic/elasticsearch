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
package org.elasticsearch.index.mapper.core;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LegacyLongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.core.LegacyDateFieldMapper.DateFieldType;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class LegacyDateFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new LegacyDateFieldMapper.DateFieldType();
    }

    @Before
    public void setupProperties() {
        setDummyNullValue(10);
        addModifier(new Modifier("format", true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((LegacyDateFieldMapper.DateFieldType) ft).setDateTimeFormatter(Joda.forPattern("basic_week_date", Locale.ROOT));
            }
        });
        addModifier(new Modifier("locale", true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((LegacyDateFieldMapper.DateFieldType) ft).setDateTimeFormatter(Joda.forPattern("date_optional_time", Locale.CANADA));
            }
        });
        addModifier(new Modifier("numeric_resolution", true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((LegacyDateFieldMapper.DateFieldType)ft).setTimeUnit(TimeUnit.HOURS);
            }
        });
    }

    public void testIsFieldWithinQueryEmptyReader() throws IOException {
        IndexReader reader = new MultiReader();
        DateFieldType ft = new DateFieldType();
        ft.setName("my_date");
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                randomBoolean(), randomBoolean(), null, null));
    }

    private void doTestIsFieldWithinQuery(DateFieldType ft, DirectoryReader reader,
            DateTimeZone zone, DateMathParser alternateFormat) throws IOException {
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-09", "2016-01-02",
                randomBoolean(), randomBoolean(), null, null));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2016-01-02", "2016-06-20",
                randomBoolean(), randomBoolean(), null, null));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2016-01-02", "2016-02-12",
                randomBoolean(), randomBoolean(), null, null));
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2014-01-02", "2015-02-12",
                randomBoolean(), randomBoolean(), null, null));
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2016-05-11", "2016-08-30",
                randomBoolean(), randomBoolean(), null, null));
        assertEquals(Relation.WITHIN, ft.isFieldWithinQuery(reader, "2015-09-25", "2016-05-29",
                randomBoolean(), randomBoolean(), null, null));
        assertEquals(Relation.WITHIN, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                true, true, null, null));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                false, false, null, null));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                false, true, null, null));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                true, false, null, null));
    }

    public void testIsFieldWithinQuery() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        long instant1 = LegacyDateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime("2015-10-12").getMillis();
        long instant2 = LegacyDateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime("2016-04-03").getMillis();
        Document doc = new Document();
        LegacyLongField field = new LegacyLongField("my_date", instant1, Store.NO);
        doc.add(field);
        w.addDocument(doc);
        field.setLongValue(instant2);
        w.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(w);
        DateFieldType ft = new DateFieldType();
        ft.setName("my_date");
        DateMathParser alternateFormat = new DateMathParser(LegacyDateFieldMapper.Defaults.DATE_TIME_FORMATTER);
        doTestIsFieldWithinQuery(ft, reader, null, null);
        doTestIsFieldWithinQuery(ft, reader, null, alternateFormat);
        doTestIsFieldWithinQuery(ft, reader, DateTimeZone.UTC, null);
        doTestIsFieldWithinQuery(ft, reader, DateTimeZone.UTC, alternateFormat);
        IOUtils.close(reader, w, dir);
    }

    public void testValueFormat() {
        MappedFieldType ft = createDefaultFieldType();
        long instant = LegacyDateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime("2015-10-12T14:10:55").getMillis();
        assertEquals("2015-10-12T14:10:55.000Z",
                ft.docValueFormat(null, DateTimeZone.UTC).format(instant));
        assertEquals("2015-10-12T15:10:55.000+01:00",
                ft.docValueFormat(null, DateTimeZone.forOffsetHours(1)).format(instant));
        assertEquals("2015",
                createDefaultFieldType().docValueFormat("YYYY", DateTimeZone.UTC).format(instant));
        assertEquals(instant,
                ft.docValueFormat(null, DateTimeZone.UTC).parseLong("2015-10-12T14:10:55", false, null));
        assertEquals(instant,
                ft.docValueFormat(null, DateTimeZone.UTC).parseLong("2015-10-12T14:10:55", true, null));
        assertEquals(LegacyDateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime("2015-10-13").getMillis() - 1,
                ft.docValueFormat(null, DateTimeZone.UTC).parseLong("2015-10-12||/d", true, null));
    }

    public void testValueForSearch() {
        MappedFieldType ft = createDefaultFieldType();
        String date = "2015-10-12T12:09:55.000Z";
        long instant = LegacyDateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime(date).getMillis();
        assertEquals(date, ft.valueForSearch(instant));
    }
}
