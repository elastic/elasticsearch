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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.plain.*;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

public class IndexFieldDataServiceTests extends ESSingleNodeTestCase {

    private static Settings DOC_VALUES_SETTINGS = Settings.builder().put(FieldDataType.FORMAT_KEY, FieldDataType.DOC_VALUES_FORMAT_VALUE).build();

    public void testGetForFieldDefaults() {
        final IndexService indexService = createIndex("test");
        final IndexFieldDataService ifdService = indexService.fieldData();
        for (boolean docValues : Arrays.asList(true, false)) {
            final BuilderContext ctx = new BuilderContext(indexService.settingsService().getSettings(), new ContentPath(1));
            final MappedFieldType stringMapper = new StringFieldMapper.Builder("string").tokenized(false).docValues(docValues).build(ctx).fieldType();
            ifdService.clear();
            IndexFieldData<?> fd = ifdService.getForField(stringMapper);
            if (docValues) {
                assertTrue(fd instanceof SortedSetDVOrdinalsIndexFieldData);
            } else {
                assertTrue(fd instanceof PagedBytesIndexFieldData);
            }

            for (MappedFieldType mapper : Arrays.asList(
                    new ByteFieldMapper.Builder("int").docValues(docValues).build(ctx).fieldType(),
                    new ShortFieldMapper.Builder("int").docValues(docValues).build(ctx).fieldType(),
                    new IntegerFieldMapper.Builder("int").docValues(docValues).build(ctx).fieldType(),
                    new LongFieldMapper.Builder("long").docValues(docValues).build(ctx).fieldType()
                    )) {
                ifdService.clear();
                fd = ifdService.getForField(mapper);
                if (docValues) {
                    assertTrue(fd instanceof SortedNumericDVIndexFieldData);
                } else {
                    assertTrue(fd instanceof PackedArrayIndexFieldData);
                }
            }

            final MappedFieldType floatMapper = new FloatFieldMapper.Builder("float").docValues(docValues).build(ctx).fieldType();
            ifdService.clear();
            fd = ifdService.getForField(floatMapper);
            if (docValues) {
                assertTrue(fd instanceof SortedNumericDVIndexFieldData);
            } else {
                assertTrue(fd instanceof FloatArrayIndexFieldData);
            }

            final MappedFieldType doubleMapper = new DoubleFieldMapper.Builder("double").docValues(docValues).build(ctx).fieldType();
            ifdService.clear();
            fd = ifdService.getForField(doubleMapper);
            if (docValues) {
                assertTrue(fd instanceof SortedNumericDVIndexFieldData);
            } else {
                assertTrue(fd instanceof DoubleArrayIndexFieldData);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testByPassDocValues() {
        final IndexService indexService = createIndex("test");
        final IndexFieldDataService ifdService = indexService.fieldData();
        final BuilderContext ctx = new BuilderContext(indexService.settingsService().getSettings(), new ContentPath(1));
        final MappedFieldType stringMapper = MapperBuilders.stringField("string").tokenized(false).fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(Settings.builder().put("format", "disabled").build()).build(ctx).fieldType();
        ifdService.clear();
        IndexFieldData<?> fd = ifdService.getForField(stringMapper);
        assertTrue(fd instanceof DisabledIndexFieldData);

        final Settings fdSettings = Settings.builder().put("format", "array").build();
        for (MappedFieldType mapper : Arrays.asList(
                new ByteFieldMapper.Builder("int").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx).fieldType(),
                new ShortFieldMapper.Builder("int").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx).fieldType(),
                new IntegerFieldMapper.Builder("int").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx).fieldType(),
                new LongFieldMapper.Builder("long").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx).fieldType()
                )) {
            ifdService.clear();
            fd = ifdService.getForField(mapper);
            assertTrue(fd instanceof PackedArrayIndexFieldData);
        }

        final MappedFieldType floatMapper = MapperBuilders.floatField("float").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(floatMapper);
        assertTrue(fd instanceof FloatArrayIndexFieldData);

        final MappedFieldType doubleMapper = MapperBuilders.doubleField("double").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(doubleMapper);
        assertTrue(fd instanceof DoubleArrayIndexFieldData);
    }

    public void testChangeFieldDataFormat() throws Exception {
        final IndexService indexService = createIndex("test");
        final IndexFieldDataService ifdService = indexService.fieldData();
        final BuilderContext ctx = new BuilderContext(indexService.settingsService().getSettings(), new ContentPath(1));
        final MappedFieldType mapper1 = MapperBuilders.stringField("s").tokenized(false).docValues(true).fieldDataSettings(Settings.builder().put(FieldDataType.FORMAT_KEY, "paged_bytes").build()).build(ctx).fieldType();
        final IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("s", "thisisastring", Store.NO));
        writer.addDocument(doc);
        final IndexReader reader1 = DirectoryReader.open(writer, true);
        IndexFieldData<?> ifd = ifdService.getForField(mapper1);
        assertThat(ifd, instanceOf(PagedBytesIndexFieldData.class));
        Set<LeafReader> oldSegments = Collections.newSetFromMap(new IdentityHashMap<LeafReader, Boolean>());
        for (LeafReaderContext arc : reader1.leaves()) {
            oldSegments.add(arc.reader());
            AtomicFieldData afd = ifd.load(arc);
            assertThat(afd, instanceOf(PagedBytesAtomicFieldData.class));
        }
        // write new segment
        writer.addDocument(doc);
        final IndexReader reader2 = DirectoryReader.open(writer, true);
        final MappedFieldType mapper2 = MapperBuilders.stringField("s").tokenized(false).docValues(true).fieldDataSettings(Settings.builder().put(FieldDataType.FORMAT_KEY, "doc_values").build()).build(ctx).fieldType();
        ifd = ifdService.getForField(mapper2);
        assertThat(ifd, instanceOf(SortedSetDVOrdinalsIndexFieldData.class));
        reader1.close();
        reader2.close();
        writer.close();
        writer.getDirectory().close();
    }

}
