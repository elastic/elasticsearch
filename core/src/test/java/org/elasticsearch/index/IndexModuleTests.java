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
package org.elasticsearch.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.test.engine.MockEngineFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexModuleTests extends ModuleTestCase {

    public void testWrapperIsBound() {
        IndexModule module = new IndexModule(Settings.EMPTY, IndexMetaData.PROTO);
        assertInstanceBinding(module, IndexSearcherWrapper.class,(x) -> x == null);
        module.indexSearcherWrapper = Wrapper.class;
        assertBinding(module, IndexSearcherWrapper.class, Wrapper.class);
    }

    public void testEngineFactoryBound() {
        IndexModule module = new IndexModule(Settings.EMPTY,IndexMetaData.PROTO);
        assertBinding(module, EngineFactory.class, InternalEngineFactory.class);
        module.engineFactoryImpl = MockEngineFactory.class;
        assertBinding(module, EngineFactory.class, MockEngineFactory.class);
    }

    public void testOtherServiceBound() {
        final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        final IndexEventListener listener = new IndexEventListener() {
            @Override
            public void beforeIndexDeleted(IndexService indexService) {
                atomicBoolean.set(true);
            }
        };
        final IndexMetaData meta = IndexMetaData.builder(IndexMetaData.PROTO).index("foo").build();
        IndexModule module = new IndexModule(Settings.EMPTY,meta);
        module.addIndexEventListener(listener);
        assertBinding(module, IndexService.class, IndexService.class);
        assertBinding(module, IndexServicesProvider.class, IndexServicesProvider.class);
        assertInstanceBinding(module, IndexMetaData.class, (x) -> x == meta);
        assertInstanceBinding(module, IndexEventListener.class, (x) -> {x.beforeIndexDeleted(null); return atomicBoolean.get();});
    }

    public static final class Wrapper extends IndexSearcherWrapper {

        @Override
        public DirectoryReader wrap(DirectoryReader reader) {
            return null;
        }

        @Override
        public IndexSearcher wrap(EngineConfig engineConfig, IndexSearcher searcher) throws EngineException {
            return null;
        }
    }

}
