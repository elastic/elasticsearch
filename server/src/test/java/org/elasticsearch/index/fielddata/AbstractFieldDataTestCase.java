/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public abstract class AbstractFieldDataTestCase extends ESSingleNodeTestCase {

    protected IndexService indexService;
    protected MapperService mapperService;
    protected IndexWriter writer;
    protected List<LeafReaderContext> readerContexts = null;
    protected DirectoryReader topLevelReader = null;
    protected IndicesFieldDataCache indicesFieldDataCache;
    protected SearchExecutionContext searchExecutionContext;

    protected abstract String getFieldDataType();

    protected boolean hasDocValues() {
        return false;
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(String fieldName) {
        return getForField(getFieldDataType(), fieldName, hasDocValues());
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(String type, String fieldName) {
        return getForField(type, fieldName, hasDocValues());
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(String type, String fieldName, boolean docValues) {
        final MappedFieldType fieldType;
        final MapperBuilderContext context = MapperBuilderContext.ROOT;
        if (type.equals("string")) {
            if (docValues) {
                fieldType = new KeywordFieldMapper.Builder(fieldName, Version.CURRENT).build(context).fieldType();
            } else {
                fieldType = new TextFieldMapper.Builder(fieldName, createDefaultIndexAnalyzers()).fielddata(true)
                    .build(context)
                    .fieldType();
            }
        } else if (type.equals("float")) {
            fieldType = new NumberFieldMapper.Builder(
                fieldName,
                NumberFieldMapper.NumberType.FLOAT,
                ScriptCompiler.NONE,
                false,
                true,
                Version.CURRENT
            ).docValues(docValues).build(context).fieldType();
        } else if (type.equals("double")) {
            fieldType = new NumberFieldMapper.Builder(
                fieldName,
                NumberFieldMapper.NumberType.DOUBLE,
                ScriptCompiler.NONE,
                false,
                true,
                Version.CURRENT
            ).docValues(docValues).build(context).fieldType();
        } else if (type.equals("long")) {
            fieldType = new NumberFieldMapper.Builder(
                fieldName,
                NumberFieldMapper.NumberType.LONG,
                ScriptCompiler.NONE,
                false,
                true,
                Version.CURRENT
            ).docValues(docValues).build(context).fieldType();
        } else if (type.equals("int")) {
            fieldType = new NumberFieldMapper.Builder(
                fieldName,
                NumberFieldMapper.NumberType.INTEGER,
                ScriptCompiler.NONE,
                false,
                true,
                Version.CURRENT
            ).docValues(docValues).build(context).fieldType();
        } else if (type.equals("short")) {
            fieldType = new NumberFieldMapper.Builder(
                fieldName,
                NumberFieldMapper.NumberType.SHORT,
                ScriptCompiler.NONE,
                false,
                true,
                Version.CURRENT
            ).docValues(docValues).build(context).fieldType();
        } else if (type.equals("byte")) {
            fieldType = new NumberFieldMapper.Builder(
                fieldName,
                NumberFieldMapper.NumberType.BYTE,
                ScriptCompiler.NONE,
                false,
                true,
                Version.CURRENT
            ).docValues(docValues).build(context).fieldType();
        } else if (type.equals("geo_point")) {
            fieldType = new GeoPointFieldMapper.Builder(fieldName, ScriptCompiler.NONE, false, Version.CURRENT).docValues(docValues)
                .build(context)
                .fieldType();
        } else if (type.equals("binary")) {
            fieldType = new BinaryFieldMapper.Builder(fieldName, docValues).build(context).fieldType();
        } else {
            throw new UnsupportedOperationException(type);
        }
        return searchExecutionContext.getForField(fieldType);
    }

    @Before
    public void setup() throws Exception {
        indexService = createIndex("test", Settings.builder().build());
        mapperService = indexService.mapperService();
        indicesFieldDataCache = getInstanceFromNode(IndicesService.class).getIndicesFieldDataCache();
        // LogByteSizeMP to preserve doc ID order
        writer = new IndexWriter(
            new ByteBuffersDirectory(),
            new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
        );
        searchExecutionContext = indexService.newSearchExecutionContext(0, 0, null, () -> 0, null, emptyMap());
    }

    protected final List<LeafReaderContext> refreshReader() throws Exception {
        if (readerContexts != null && topLevelReader != null) {
            topLevelReader.close();
        }
        topLevelReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        readerContexts = topLevelReader.leaves();
        return readerContexts;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (topLevelReader != null) {
            topLevelReader.close();
        }
        writer.close();
        searchExecutionContext = null;
    }

    protected Nested createNested(IndexSearcher searcher, Query parentFilter, Query childFilter) throws IOException {
        BitsetFilterCache s = indexService.cache().bitsetFilterCache();
        return new Nested(s.getBitSetProducer(parentFilter), childFilter, null, searcher);
    }

    public void testEmpty() throws Exception {
        Document d = new Document();
        d.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(d);
        refreshReader();

        IndexFieldData<?> fieldData = getForField("non_existing_field");
        int max = randomInt(7);
        for (LeafReaderContext readerContext : readerContexts) {
            LeafFieldData previous = null;
            for (int i = 0; i < max; i++) {
                LeafFieldData current = fieldData.load(readerContext);
                assertThat(current.ramBytesUsed(), equalTo(0L));
                if (previous != null) {
                    assertThat(current, not(sameInstance(previous)));
                }
                previous = current;
            }
        }
    }
}
