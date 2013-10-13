/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.index.fielddata;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.plain.*;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;

public class IndexFieldDataServiceTests extends ElasticsearchTestCase {

    private static Settings DOC_VALUES_SETTINGS = ImmutableSettings.builder().put(FieldDataType.FORMAT_KEY, FieldDataType.DOC_VALUES_FORMAT_VALUE).build();

    @SuppressWarnings("unchecked")
    public void testGetForFieldDefaults() {
        final IndexFieldDataService ifdService = new IndexFieldDataService(new Index("test"));
        for (boolean docValues : Arrays.asList(true, false)) {
            final BuilderContext ctx = new BuilderContext(null, new ContentPath(1));
            final StringFieldMapper stringMapper = new StringFieldMapper.Builder("string").tokenized(false).fieldDataSettings(docValues ? DOC_VALUES_SETTINGS : ImmutableSettings.EMPTY).build(ctx);
            ifdService.clear();
            IndexFieldData<?> fd = ifdService.getForField(stringMapper);
            if (docValues) {
                assertTrue(fd instanceof SortedSetDVBytesIndexFieldData);
            } else {
                assertTrue(fd instanceof PagedBytesIndexFieldData);
            }

            for (FieldMapper<?> mapper : Arrays.asList(
                    new ByteFieldMapper.Builder("int").fieldDataSettings(docValues ? DOC_VALUES_SETTINGS : ImmutableSettings.EMPTY).build(ctx),
                    new ShortFieldMapper.Builder("int").fieldDataSettings(docValues ? DOC_VALUES_SETTINGS : ImmutableSettings.EMPTY).build(ctx),
                    new IntegerFieldMapper.Builder("int").fieldDataSettings(docValues ? DOC_VALUES_SETTINGS : ImmutableSettings.EMPTY).build(ctx),
                    new LongFieldMapper.Builder("long").fieldDataSettings(docValues ? DOC_VALUES_SETTINGS : ImmutableSettings.EMPTY).build(ctx)
                    )) {
                ifdService.clear();
                fd = ifdService.getForField(mapper);
                if (docValues) {
                    assertTrue(fd instanceof SortedSetDVNumericIndexFieldData);
                } else {
                    assertTrue(fd instanceof PackedArrayIndexFieldData);
                }
            }

            final FloatFieldMapper floatMapper = new FloatFieldMapper.Builder("float").fieldDataSettings(docValues ? DOC_VALUES_SETTINGS : ImmutableSettings.EMPTY).build(ctx);
            ifdService.clear();
            fd = ifdService.getForField(floatMapper);
            if (docValues) {
                assertTrue(fd instanceof SortedSetDVNumericIndexFieldData);
            } else {
                assertTrue(fd instanceof FloatArrayIndexFieldData);
            }

            final DoubleFieldMapper doubleMapper = new DoubleFieldMapper.Builder("double").fieldDataSettings(docValues ? DOC_VALUES_SETTINGS : ImmutableSettings.EMPTY).build(ctx);
            ifdService.clear();
            fd = ifdService.getForField(doubleMapper);
            if (docValues) {
                assertTrue(fd instanceof SortedSetDVNumericIndexFieldData);
            } else {
                assertTrue(fd instanceof DoubleArrayIndexFieldData);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testByPassDocValues() {
        final IndexFieldDataService ifdService = new IndexFieldDataService(new Index("test"));
        final BuilderContext ctx = new BuilderContext(null, new ContentPath(1));
        final StringFieldMapper stringMapper = new StringFieldMapper.Builder("string").tokenized(false).fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(ImmutableSettings.builder().put("format", "fst").build()).build(ctx);
        ifdService.clear();
        IndexFieldData<?> fd = ifdService.getForField(stringMapper);
        assertTrue(fd instanceof FSTBytesIndexFieldData);

        final Settings fdSettings = ImmutableSettings.builder().put("format", "array").build();
        for (FieldMapper<?> mapper : Arrays.asList(
                new ByteFieldMapper.Builder("int").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx),
                new ShortFieldMapper.Builder("int").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx),
                new IntegerFieldMapper.Builder("int").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx),
                new LongFieldMapper.Builder("long").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx)
                )) {
            ifdService.clear();
            fd = ifdService.getForField(mapper);
            assertTrue(fd instanceof PackedArrayIndexFieldData);
        }

        final FloatFieldMapper floatMapper = new FloatFieldMapper.Builder("float").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx);
        ifdService.clear();
        fd = ifdService.getForField(floatMapper);
        assertTrue(fd instanceof FloatArrayIndexFieldData);

        final DoubleFieldMapper doubleMapper = new DoubleFieldMapper.Builder("double").fieldDataSettings(DOC_VALUES_SETTINGS).fieldDataSettings(fdSettings).build(ctx);
        ifdService.clear();
        fd = ifdService.getForField(doubleMapper);
        assertTrue(fd instanceof DoubleArrayIndexFieldData);
    }

}
