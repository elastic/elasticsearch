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

import org.elasticsearch.index.cache.bitset.BitsetFilterCache;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Filter;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;

public abstract class AbstractFieldDataTests extends ElasticsearchSingleNodeTest {

    protected IndexService indexService;
    protected IndexFieldDataService ifdService;
    protected MapperService mapperService;
    protected IndexWriter writer;
    protected LeafReaderContext readerContext;
    protected IndexReader topLevelReader;
    protected IndicesFieldDataCache indicesFieldDataCache;

    protected abstract FieldDataType getFieldDataType();

    protected boolean hasDocValues() {
        return false;
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(String fieldName) {
        return getForField(getFieldDataType(), fieldName, hasDocValues());
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(FieldDataType type, String fieldName) {
        return getForField(type, fieldName, hasDocValues());
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(FieldDataType type, String fieldName, boolean docValues) {
        final FieldMapper<?> mapper;
        final BuilderContext context = new BuilderContext(indexService.settingsService().getSettings(), new ContentPath(1));
        if (type.getType().equals("string")) {
            mapper = MapperBuilders.stringField(fieldName).tokenized(false).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("float")) {
            mapper = MapperBuilders.floatField(fieldName).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("double")) {
            mapper = MapperBuilders.doubleField(fieldName).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("long")) {
            mapper = MapperBuilders.longField(fieldName).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("int")) {
            mapper = MapperBuilders.integerField(fieldName).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("short")) {
            mapper = MapperBuilders.shortField(fieldName).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("byte")) {
            mapper = MapperBuilders.byteField(fieldName).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("geo_point")) {
            mapper = MapperBuilders.geoPointField(fieldName).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("_parent")) {
            mapper = MapperBuilders.parent().type(fieldName).build(context);
        } else if (type.getType().equals("binary")) {
            mapper = MapperBuilders.binaryField(fieldName).docValues(docValues).fieldDataSettings(type.getSettings()).build(context);
        } else {
            throw new UnsupportedOperationException(type.getType());
        }
        return ifdService.getForField(mapper);
    }

    @Before
    public void setup() throws Exception {
        Settings settings = ImmutableSettings.builder().put("index.fielddata.cache", "none").build();
        indexService = createIndex("test", settings);
        mapperService = indexService.mapperService();
        indicesFieldDataCache = indexService.injector().getInstance(IndicesFieldDataCache.class);
        ifdService = indexService.fieldData();
        // LogByteSizeMP to preserve doc ID order
        writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy()));
    }

    protected LeafReaderContext refreshReader() throws Exception {
        if (readerContext != null) {
            readerContext.reader().close();
        }
        LeafReader reader = SlowCompositeReaderWrapper.wrap(topLevelReader = DirectoryReader.open(writer, true));
        readerContext = reader.getContext();
        return readerContext;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (readerContext != null) {
            readerContext.reader().close();
        }
        writer.close();
    }

    protected Nested createNested(Filter parentFilter, Filter childFilter) {
        BitsetFilterCache s = indexService.bitsetFilterCache();
        return new Nested(s.getBitDocIdSetFilter(parentFilter), s.getBitDocIdSetFilter(childFilter));
    }

}
