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

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;

public class TranslogHandler extends TranslogRecoveryPerformer {

    private final MapperService mapperService;
    public Mapping mappingUpdate = null;

    public final AtomicInteger recoveredOps = new AtomicInteger(0);

    public TranslogHandler(NamedXContentRegistry xContentRegistry, String indexName, Logger logger) {
        super(new ShardId("test", "_na_", 0), null, logger);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        Index index = new Index(indexName, "_na_");
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexAnalyzers indexAnalyzers = null;
        NamedAnalyzer defaultAnalyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
        indexAnalyzers = new IndexAnalyzers(indexSettings, defaultAnalyzer, defaultAnalyzer, defaultAnalyzer, emptyMap(), emptyMap());
        SimilarityService similarityService = new SimilarityService(indexSettings, emptyMap());
        MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
        mapperService = new MapperService(indexSettings, indexAnalyzers, xContentRegistry, similarityService, mapperRegistry,
                () -> null);
    }

    @Override
    protected DocumentMapperForType docMapper(String type) {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder(type);
        DocumentMapper.Builder b = new DocumentMapper.Builder(rootBuilder, mapperService);
        return new DocumentMapperForType(b.build(mapperService), mappingUpdate);
    }

    @Override
    protected void operationProcessed() {
        recoveredOps.incrementAndGet();
    }
}
