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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.breaker.DummyCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCacheListener;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.index.service.StubIndexService;
import org.junit.After;
import org.junit.Before;

// we might wanna cut this over to LuceneTestCase
public abstract class AbstractFieldDataTests extends ElasticsearchTestCase {

    protected IndexFieldDataService ifdService;
    protected IndexWriter writer;
    protected AtomicReaderContext readerContext;
    protected IndexReader topLevelReader;
    protected IndicesFieldDataCache indicesFieldDataCache;

    protected abstract FieldDataType getFieldDataType();

    protected boolean hasDocValues() {
        return false;
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(String fieldName) {
        return getForField(getFieldDataType(), fieldName);
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(FieldDataType type, String fieldName) {
        final FieldMapper<?> mapper;
        final BuilderContext context = new BuilderContext(null, new ContentPath(1));
        if (type.getType().equals("string")) {
            mapper = MapperBuilders.stringField(fieldName).tokenized(false).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("float")) {
            mapper = MapperBuilders.floatField(fieldName).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("double")) {
            mapper = MapperBuilders.doubleField(fieldName).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("long")) {
            mapper = MapperBuilders.longField(fieldName).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("int")) {
            mapper = MapperBuilders.integerField(fieldName).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("short")) {
            mapper = MapperBuilders.shortField(fieldName).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("byte")) {
            mapper = MapperBuilders.byteField(fieldName).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("geo_point")) {
            mapper = MapperBuilders.geoPointField(fieldName).fieldDataSettings(type.getSettings()).build(context);
        } else if (type.getType().equals("_parent")) {
            mapper = MapperBuilders.parent().type(fieldName).build(context);
        } else if (type.getType().equals("binary")) {
            mapper = MapperBuilders.binaryField(fieldName).fieldDataSettings(type.getSettings()).build(context);
        } else {
            throw new UnsupportedOperationException(type.getType());
        }
        return ifdService.getForField(mapper);
    }

    @Before
    public void setup() throws Exception {
        CircuitBreakerService circuitBreakerService = new DummyCircuitBreakerService();
        indicesFieldDataCache = new IndicesFieldDataCache(
                ImmutableSettings.Builder.EMPTY_SETTINGS,
                new IndicesFieldDataCacheListener(circuitBreakerService)
        );
        ifdService = new IndexFieldDataService(new Index("test"), circuitBreakerService, indicesFieldDataCache);
        MapperService mapperService = MapperTestUtils.newMapperService(ifdService.index(), ImmutableSettings.Builder.EMPTY_SETTINGS);
        ifdService.setIndexService(new StubIndexService(mapperService));
        // LogByteSizeMP to preserve doc ID order
        writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION)).setMergePolicy(new LogByteSizeMergePolicy()));
    }

    protected AtomicReaderContext refreshReader() throws Exception {
        if (readerContext != null) {
            readerContext.reader().close();
        }
        AtomicReader reader = SlowCompositeReaderWrapper.wrap(topLevelReader = DirectoryReader.open(writer, true));
        readerContext = reader.getContext();
        return readerContext;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (readerContext != null) {
            readerContext.reader().close();
        }
        writer.close();
        ifdService.clear();
    }

}
