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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.mapper.SourceToParse.source;

public class TranslogHandler implements EngineConfig.TranslogRecoveryRunner {

    private final MapperService mapperService;
    public Mapping mappingUpdate = null;
    private final Map<String, Mapping> recoveredTypes = new HashMap<>();

    private final AtomicLong appliedOperations = new AtomicLong();

    long appliedOperations() {
        return appliedOperations.get();
    }

    public TranslogHandler(NamedXContentRegistry xContentRegistry, IndexSettings indexSettings) {
        NamedAnalyzer defaultAnalyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
        IndexAnalyzers indexAnalyzers =
                new IndexAnalyzers(indexSettings, defaultAnalyzer, defaultAnalyzer, defaultAnalyzer, emptyMap(), emptyMap());
        SimilarityService similarityService = new SimilarityService(indexSettings, null, emptyMap());
        MapperRegistry mapperRegistry = new IndicesModule(emptyList()).getMapperRegistry();
        mapperService = new MapperService(indexSettings, indexAnalyzers, xContentRegistry, similarityService, mapperRegistry,
                () -> null);
    }

    private DocumentMapperForType docMapper(String type) {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder(type);
        DocumentMapper.Builder b = new DocumentMapper.Builder(rootBuilder, mapperService);
        return new DocumentMapperForType(b.build(mapperService), mappingUpdate);
    }

    private void applyOperation(Engine engine, Engine.Operation operation) throws IOException {
        switch (operation.operationType()) {
            case INDEX:
                Engine.Index engineIndex = (Engine.Index) operation;
                Mapping update = engineIndex.parsedDoc().dynamicMappingsUpdate();
                if (engineIndex.parsedDoc().dynamicMappingsUpdate() != null) {
                    recoveredTypes.compute(engineIndex.type(), (k, mapping) -> mapping == null ? update : mapping.merge(update, false));
                }
                engine.index(engineIndex);
                break;
            case DELETE:
                engine.delete((Engine.Delete) operation);
                break;
            case NO_OP:
                engine.noOp((Engine.NoOp) operation);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
    }

    /**
     * Returns the recovered types modifying the mapping during the recovery
     */
    public Map<String, Mapping> getRecoveredTypes() {
        return recoveredTypes;
    }

    @Override
    public int run(Engine engine, Translog.Snapshot snapshot) throws IOException {
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            applyOperation(engine, convertToEngineOp(operation, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY));
            opsRecovered++;
            appliedOperations.incrementAndGet();
        }
        return opsRecovered;
    }

    private Engine.Operation convertToEngineOp(Translog.Operation operation, Engine.Operation.Origin origin) {
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                final String indexName = mapperService.index().getName();
                final Engine.Index engineIndex = IndexShard.prepareIndex(docMapper(index.type()),
                        mapperService.getIndexSettings().getIndexVersionCreated(),
                        source(indexName, index.type(), index.id(), index.source(), XContentFactory.xContentType(index.source()))
                                .routing(index.routing()).parent(index.parent()), index.seqNo(), index.primaryTerm(),
                        index.version(), index.versionType().versionTypeForReplicationAndRecovery(), origin,
                        index.getAutoGeneratedIdTimestamp(), true);
                return engineIndex;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                final Engine.Delete engineDelete = new Engine.Delete(delete.type(), delete.id(), delete.uid(), delete.seqNo(),
                        delete.primaryTerm(), delete.version(), delete.versionType().versionTypeForReplicationAndRecovery(),
                        origin, System.nanoTime());
                return engineDelete;
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                final Engine.NoOp engineNoOp =
                        new Engine.NoOp(noOp.seqNo(), noOp.primaryTerm(), origin, System.nanoTime(), noOp.reason());
                return engineNoOp;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
    }

}
