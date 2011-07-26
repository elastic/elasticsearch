/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.get;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.selector.FieldMappersFieldSelector;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.*;

/**
 * Performs the get operation.
 *
 * @author kimchy (shay.banon)
 */
public class TransportGetAction extends TransportShardSingleOperationAction<GetRequest, GetResponse> {

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final boolean realtime;

    @Inject public TransportGetAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ScriptService scriptService, ThreadPool threadPool) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.scriptService = scriptService;

        this.realtime = settings.getAsBoolean("action.get.realtime", true);
    }

    @Override protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    @Override protected String transportAction() {
        return TransportActions.GET;
    }

    @Override protected String transportShardAction() {
        return "indices/get/shard";
    }

    @Override protected void checkBlock(GetRequest request, ClusterState state) {
        state.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, request.index());
    }

    @Override protected ShardIterator shards(ClusterState clusterState, GetRequest request) {
        return clusterService.operationRouting()
                .getShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing(), request.preference());
    }

    @Override protected void doExecute(GetRequest request, ActionListener<GetResponse> listener) {
        if (request.realtime == null) {
            request.realtime = this.realtime;
        }
        // update the routing (request#index here is possibly an alias)
        MetaData metaData = clusterService.state().metaData();
        request.routing(metaData.resolveIndexRouting(request.routing(), request.index()));

        super.doExecute(request, listener);
    }

    @Override protected GetResponse shardOperation(GetRequest request, int shardId) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);

        if (request.refresh() && !request.realtime()) {
            indexShard.refresh(new Engine.Refresh(false));
        }

        return load(logger, scriptService, indexService, indexShard, request.index(), request.type(), request.id(), request.fields(), request.realtime());
    }

    public static GetResponse load(ESLogger logger, ScriptService scriptService, IndexService indexService, IndexShard indexShard, String index, String type, String id, String[] gFields, boolean realtime) throws ElasticSearchException {
        boolean loadSource = gFields == null || gFields.length > 0;
        Engine.GetResult get = null;
        if (type == null || type.equals("_all")) {
            for (String typeX : indexService.mapperService().types()) {
                get = indexShard.get(new Engine.Get(realtime, UidFieldMapper.TERM_FACTORY.createTerm(Uid.createUid(typeX, id))).loadSource(loadSource));
                if (get.exists()) {
                    type = typeX;
                    break;
                }
            }
            if (get == null || !get.exists()) {
                return new GetResponse(index, type, id, -1, false, null, null);
            }
        } else {
            get = indexShard.get(new Engine.Get(realtime, UidFieldMapper.TERM_FACTORY.createTerm(Uid.createUid(type, id))).loadSource(loadSource));
            if (!get.exists()) {
                return new GetResponse(index, type, id, -1, false, null, null);
            }
        }

        DocumentMapper docMapper = indexService.mapperService().documentMapper(type);
        if (docMapper == null) {
            return new GetResponse(index, type, id, -1, false, null, null);
        }

        try {
            // break between having loaded it from translog (so we only have _source), and having a document to load
            if (get.docIdAndVersion() != null) {
                Map<String, GetField> fields = null;
                byte[] source = null;
                UidField.DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
                FieldSelector fieldSelector = buildFieldSelectors(docMapper, gFields);
                if (fieldSelector != null) {
                    Document doc;
                    try {
                        doc = docIdAndVersion.reader.document(docIdAndVersion.docId, fieldSelector);
                    } catch (IOException e) {
                        throw new ElasticSearchException("Failed to get type [" + type + "] and id [" + id + "]", e);
                    }
                    source = extractSource(doc, docMapper);

                    for (Object oField : doc.getFields()) {
                        Fieldable field = (Fieldable) oField;
                        String name = field.name();
                        Object value = null;
                        FieldMappers fieldMappers = docMapper.mappers().indexName(field.name());
                        if (fieldMappers != null) {
                            FieldMapper mapper = fieldMappers.mapper();
                            if (mapper != null) {
                                name = mapper.names().fullName();
                                value = mapper.valueForSearch(field);
                            }
                        }
                        if (value == null) {
                            if (field.isBinary()) {
                                value = field.getBinaryValue();
                            } else {
                                value = field.stringValue();
                            }
                        }

                        if (fields == null) {
                            fields = newHashMapWithExpectedSize(2);
                        }

                        GetField getField = fields.get(name);
                        if (getField == null) {
                            getField = new GetField(name, new ArrayList<Object>(2));
                            fields.put(name, getField);
                        }
                        getField.values().add(value);
                    }
                }

                // now, go and do the script thingy if needed
                if (gFields != null && gFields.length > 0) {
                    SearchLookup searchLookup = null;
                    for (String field : gFields) {
                        String script = null;
                        if (field.contains("_source.") || field.contains("doc[")) {
                            script = field;
                        } else {
                            FieldMappers x = docMapper.mappers().smartName(field);
                            if (x != null && !x.mapper().stored()) {
                                script = "_source." + x.mapper().names().fullName();
                            }
                        }
                        if (script != null) {
                            if (searchLookup == null) {
                                searchLookup = new SearchLookup(indexService.mapperService(), indexService.cache().fieldData());
                            }
                            SearchScript searchScript = scriptService.search(searchLookup, "mvel", script, null);
                            searchScript.setNextReader(docIdAndVersion.reader);
                            searchScript.setNextDocId(docIdAndVersion.docId);

                            try {
                                Object value = searchScript.run();
                                if (fields == null) {
                                    fields = newHashMapWithExpectedSize(2);
                                }
                                GetField getField = fields.get(field);
                                if (getField == null) {
                                    getField = new GetField(field, new ArrayList<Object>(2));
                                    fields.put(field, getField);
                                }
                                getField.values().add(value);
                            } catch (RuntimeException e) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("failed to execute get request script field [{}]", e, script);
                                }
                                // ignore
                            }
                        }
                    }
                }

                return new GetResponse(index, type, id, get.version(), get.exists(), source == null ? null : new BytesHolder(source), fields);
            } else {
                BytesHolder source = get.source();

                Map<String, GetField> fields = null;
                boolean sourceRequested = false;

                // we can only load scripts that can run against the source
                if (gFields == null) {
                    sourceRequested = true;
                } else if (gFields.length == 0) {
                    // no fields, and no source
                    sourceRequested = false;
                } else {
                    Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(source.bytes(), source.offset(), source.length());
                    SearchLookup searchLookup = null;
                    for (String field : gFields) {
                        if (field.equals("_source")) {
                            sourceRequested = true;
                            continue;
                        }
                        String script = null;
                        if (field.contains("_source.")) {
                            script = field;
                        } else {
                            FieldMappers x = docMapper.mappers().smartName(field);
                            if (x != null) {
                                script = "_source." + x.mapper().names().fullName();
                            }
                        }
                        if (script != null) {
                            if (searchLookup == null) {
                                searchLookup = new SearchLookup(indexService.mapperService(), indexService.cache().fieldData());
                            }
                            SearchScript searchScript = scriptService.search(searchLookup, "mvel", script, null);
                            // we can't do this, only allow to run scripts against the source
                            //searchScript.setNextReader(docIdAndVersion.reader);
                            //searchScript.setNextDocId(docIdAndVersion.docId);

                            // but, we need to inject the parsed source into the script, so it will be used...
                            searchScript.setNextSource(sourceAsMap);

                            try {
                                Object value = searchScript.run();
                                if (fields == null) {
                                    fields = newHashMapWithExpectedSize(2);
                                }
                                GetField getField = fields.get(field);
                                if (getField == null) {
                                    getField = new GetField(field, new ArrayList<Object>(2));
                                    fields.put(field, getField);
                                }
                                getField.values().add(value);
                            } catch (RuntimeException e) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("failed to execute get request script field [{}]", e, script);
                                }
                                // ignore
                            }
                        }
                    }
                }

                return new GetResponse(index, type, id, get.version(), get.exists(), sourceRequested ? source : null, fields);
            }
        } finally {
            if (get.searcher() != null) {
                get.searcher().release();
            }
        }
    }

    private static FieldSelector buildFieldSelectors(DocumentMapper docMapper, String... fields) {
        if (fields == null) {
            return docMapper.sourceMapper().fieldSelector();
        }

        // don't load anything
        if (fields.length == 0) {
            return null;
        }

        FieldMappersFieldSelector fieldSelector = null;
        for (String fieldName : fields) {
            FieldMappers x = docMapper.mappers().smartName(fieldName);
            if (x != null && x.mapper().stored()) {
                if (fieldSelector == null) {
                    fieldSelector = new FieldMappersFieldSelector();
                }
                fieldSelector.add(x);
            }
        }

        return fieldSelector;
    }

    private static byte[] extractSource(Document doc, DocumentMapper documentMapper) {
        byte[] source = null;
        Fieldable sourceField = doc.getFieldable(documentMapper.sourceMapper().names().indexName());
        if (sourceField != null) {
            source = documentMapper.sourceMapper().nativeValue(sourceField);
            doc.removeField(documentMapper.sourceMapper().names().indexName());
        }
        return source;
    }

    @Override protected GetRequest newRequest() {
        return new GetRequest();
    }

    @Override protected GetResponse newResponse() {
        return new GetResponse();
    }
}
