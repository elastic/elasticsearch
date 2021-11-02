/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

public class TypeFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDocValuesSingleType() throws Exception {
        testDocValues(this::createIndex);
        assertWarnings("[types removal] Using the _type field in queries and aggregations is deprecated, prefer to use a field instead.");
    }

    public static void testDocValues(Function<String, IndexService> createIndex) throws IOException {
        MapperService mapperService = createIndex.apply("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE);
        ParsedDocument document = mapper.parse(new SourceToParse("index", "type", "id", new BytesArray("{}"), XContentType.JSON));

        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(document.rootDoc());
        DirectoryReader r = DirectoryReader.open(w);
        w.close();

        MappedFieldType ft = mapperService.fieldType(TypeFieldMapper.NAME);
        IndexOrdinalsFieldData fd = (IndexOrdinalsFieldData) ft.fielddataBuilder(
            "test",
            () -> { throw new UnsupportedOperationException(); }
        ).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
        LeafOrdinalsFieldData afd = fd.load(r.leaves().get(0));
        SortedSetDocValues values = afd.getOrdinalsValues();
        assertTrue(values.advanceExact(0));
        assertEquals(0, values.nextOrd());
        assertEquals(SortedSetDocValues.NO_MORE_ORDS, values.nextOrd());
        assertEquals(new BytesRef("type"), values.lookupOrd(0));
        r.close();
        dir.close();
    }

    public void testDefaults() throws IOException {
        Settings indexSettings = Settings.EMPTY;
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE);
        ParsedDocument document = mapper.parse(new SourceToParse("index", "type", "id", new BytesArray("{}"), XContentType.JSON));
        assertEquals(Collections.<IndexableField>emptyList(), Arrays.asList(document.rootDoc().getFields(TypeFieldMapper.NAME)));
    }
}
