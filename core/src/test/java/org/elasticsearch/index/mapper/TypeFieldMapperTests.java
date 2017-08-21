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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class TypeFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDocValuesMultipleTypes() throws Exception {
        testDocValues(false);
    }

    public void testDocValuesSingleType() throws Exception {
        testDocValues(true);
    }

    public void testDocValues(boolean singleType) throws IOException {
        Settings indexSettings = singleType ? Settings.EMPTY : Settings.builder()
                .put("index.version.created", Version.V_5_6_0)
                .build();
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE, false);
        ParsedDocument document = mapper.parse(SourceToParse.source("index", "type", "id", new BytesArray("{}"), XContentType.JSON));

        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(document.rootDoc());
        DirectoryReader r = DirectoryReader.open(w);
        w.close();

        MappedFieldType ft = mapperService.fullName(TypeFieldMapper.NAME);
        IndexOrdinalsFieldData fd = (IndexOrdinalsFieldData) ft.fielddataBuilder("test").build(mapperService.getIndexSettings(),
                ft, new IndexFieldDataCache.None(), new NoneCircuitBreakerService(), mapperService);
        AtomicOrdinalsFieldData afd = fd.load(r.leaves().get(0));
        SortedSetDocValues values = afd.getOrdinalsValues();
        assertTrue(values.advanceExact(0));
        assertEquals(0, values.nextOrd());
        assertEquals(SortedSetDocValues.NO_MORE_ORDS, values.nextOrd());
        assertEquals(new BytesRef("type"), values.lookupOrd(0));
        r.close();
        dir.close();
    }

    public void testDefaultsMultipleTypes() throws IOException {
        Settings indexSettings = Settings.builder()
                .put("index.version.created", Version.V_5_6_0)
                .build();
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE, false);
        ParsedDocument document = mapper.parse(SourceToParse.source("index", "type", "id", new BytesArray("{}"), XContentType.JSON));
        IndexableField[] fields = document.rootDoc().getFields(TypeFieldMapper.NAME);
        assertEquals(IndexOptions.DOCS, fields[0].fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, fields[1].fieldType().docValuesType());
    }

    public void testDefaultsSingleType() throws IOException {
        Settings indexSettings = Settings.EMPTY;
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE, false);
        ParsedDocument document = mapper.parse(SourceToParse.source("index", "type", "id", new BytesArray("{}"), XContentType.JSON));
        assertEquals(Collections.<IndexableField>emptyList(), Arrays.asList(document.rootDoc().getFields(TypeFieldMapper.NAME)));
    }
}
