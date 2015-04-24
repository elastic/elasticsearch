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
package org.elasticsearch.index.shard;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IgnoreOnRecoveryEngineException;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperUtils;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.translog.Translog;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.mapper.SourceToParse.source;

/**
 * The TranslogRecoveryPerformer encapsulates all the logic needed to transform a translog entry into an
 * indexing operation including source parsing and field creation from the source.
 */
public class TranslogRecoveryPerformer {
    private final MapperService mapperService;
    private final IndexQueryParserService queryParserService;
    private final IndexAliasesService indexAliasesService;
    private final IndexCache indexCache;
    private final MapperAnalyzer mapperAnalyzer;
    private final Map<String, Mapping> recoveredTypes = new HashMap<>();

    protected TranslogRecoveryPerformer(MapperService mapperService, MapperAnalyzer mapperAnalyzer, IndexQueryParserService queryParserService, IndexAliasesService indexAliasesService, IndexCache indexCache) {
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.indexAliasesService = indexAliasesService;
        this.indexCache = indexCache;
        this.mapperAnalyzer = mapperAnalyzer;
    }

    protected Tuple<DocumentMapper, Mapping> docMapper(String type) {
        return mapperService.documentMapperWithAutoCreate(type); // protected for testing
    }

    /*
     * Applies all operations in the iterable to the current engine and returns the number of operations applied.
     * This operation will stop applying operations once an opertion failed to apply.
     */
    int performBatchRecovery(Engine engine, Iterable<Translog.Operation> operations) {
        int numOps = 0;
        for (Translog.Operation operation : operations) {
            performRecoveryOperation(engine, operation);
            numOps++;
        }
        return numOps;
    }

    private void addMappingUpdate(String type, Mapping update) {
        Mapping currentUpdate = recoveredTypes.get(type);
        if (currentUpdate == null) {
            recoveredTypes.put(type, update);
        } else {
            MapperUtils.merge(currentUpdate, update);
        }
    }

    /**
     * Performs a single recovery operation, and returns the indexing operation (or null if its not an indexing operation)
     * that can then be used for mapping updates (for example) if needed.
     */
    public void performRecoveryOperation(Engine engine, Translog.Operation operation) throws ElasticsearchException {
        try {
            switch (operation.opType()) {
                case CREATE:
                    Translog.Create create = (Translog.Create) operation;
                    Engine.Create engineCreate = IndexShard.prepareCreate(docMapper(create.type()),
                            source(create.source()).type(create.type()).id(create.id())
                                    .routing(create.routing()).parent(create.parent()).timestamp(create.timestamp()).ttl(create.ttl()),
                            create.version(), create.versionType().versionTypeForReplicationAndRecovery(), Engine.Operation.Origin.RECOVERY, true, false);
                    mapperAnalyzer.setType(create.type()); // this is a PITA - once mappings are per index not per type this can go away an we can just simply move this to the engine eventually :)
                    engine.create(engineCreate);
                    if (engineCreate.parsedDoc().dynamicMappingsUpdate() != null) {
                        addMappingUpdate(engineCreate.type(), engineCreate.parsedDoc().dynamicMappingsUpdate());
                    }
                    break;
                case SAVE:
                    Translog.Index index = (Translog.Index) operation;
                    Engine.Index engineIndex = IndexShard.prepareIndex(docMapper(index.type()), source(index.source()).type(index.type()).id(index.id())
                                    .routing(index.routing()).parent(index.parent()).timestamp(index.timestamp()).ttl(index.ttl()),
                            index.version(), index.versionType().versionTypeForReplicationAndRecovery(), Engine.Operation.Origin.RECOVERY, true);
                    mapperAnalyzer.setType(index.type());
                    engine.index(engineIndex);
                    if (engineIndex.parsedDoc().dynamicMappingsUpdate() != null) {
                        addMappingUpdate(engineIndex.type(), engineIndex.parsedDoc().dynamicMappingsUpdate());
                    }
                    break;
                case DELETE:
                    Translog.Delete delete = (Translog.Delete) operation;
                    Uid uid = Uid.createUid(delete.uid().text());
                    engine.delete(new Engine.Delete(uid.type(), uid.id(), delete.uid(), delete.version(),
                            delete.versionType().versionTypeForReplicationAndRecovery(), Engine.Operation.Origin.RECOVERY, System.nanoTime(), false));
                    break;
                case DELETE_BY_QUERY:
                    Translog.DeleteByQuery deleteByQuery = (Translog.DeleteByQuery) operation;
                    engine.delete(IndexShard.prepareDeleteByQuery(queryParserService, mapperService, indexAliasesService, indexCache,
                            deleteByQuery.source(), deleteByQuery.filteringAliases(), Engine.Operation.Origin.RECOVERY, deleteByQuery.types()));
                    break;
                default:
                    throw new ElasticsearchIllegalStateException("No operation defined for [" + operation + "]");
            }
        } catch (ElasticsearchException e) {
            boolean hasIgnoreOnRecoveryException = false;
            ElasticsearchException current = e;
            while (true) {
                if (current instanceof IgnoreOnRecoveryEngineException) {
                    hasIgnoreOnRecoveryException = true;
                    break;
                }
                if (current.getCause() instanceof ElasticsearchException) {
                    current = (ElasticsearchException) current.getCause();
                } else {
                    break;
                }
            }
            if (!hasIgnoreOnRecoveryException) {
                throw e;
            }
        }
        operationProcessed();
    }

    /**
     * Called once for every processed operation by this recovery performer.
     * This can be used to get progress information on the translog execution.
     */
    protected void operationProcessed() {
        // noop
    }

    /**
     * Returns the recovered types modifying the mapping during the recovery
     */
    public Map<String, Mapping> getRecoveredTypes() {
        return recoveredTypes;
    }
}
