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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.bloom.BloomCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
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

    @Inject public TransportGetAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ScriptService scriptService, ThreadPool threadPool) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.scriptService = scriptService;
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

    @Override protected GetResponse shardOperation(GetRequest request, int shardId) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        BloomCache bloomCache = indexService.cache().bloomCache();
        IndexShard indexShard = indexService.shardSafe(shardId);

        DocumentMapper docMapper = indexService.mapperService().documentMapper(request.type());
        if (docMapper == null) {
            throw new TypeMissingException(new Index(request.index()), request.type());
        }

        if (request.refresh()) {
            indexShard.refresh(new Engine.Refresh(false));
        }

        Engine.Searcher searcher = indexShard.searcher();
        boolean exists = false;
        byte[] source = null;
        Map<String, GetField> fields = null;
        long version = -1;
        try {
            UidField.DocIdAndVersion docIdAndVersion = loadCurrentVersionFromIndex(bloomCache, searcher, docMapper.uidMapper().term(request.type(), request.id()));
            if (docIdAndVersion != null && docIdAndVersion.docId != Lucene.NO_DOC) {
                if (docIdAndVersion.version > 0) {
                    version = docIdAndVersion.version;
                }
                exists = true;
                FieldSelector fieldSelector = buildFieldSelectors(docMapper, request.fields());
                if (fieldSelector != null) {
                    Document doc = docIdAndVersion.reader.document(docIdAndVersion.docId, fieldSelector);
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
                if (request.fields() != null && request.fields().length > 0) {
                    SearchLookup searchLookup = null;
                    for (String field : request.fields()) {
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
            }
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get type [" + request.type() + "] and id [" + request.id() + "]", e);
        } finally {
            searcher.release();
        }
        return new GetResponse(request.index(), request.type(), request.id(), version, exists, source, fields);
    }

    private FieldSelector buildFieldSelectors(DocumentMapper docMapper, String... fields) {
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

    private byte[] extractSource(Document doc, DocumentMapper documentMapper) {
        byte[] source = null;
        Fieldable sourceField = doc.getFieldable(documentMapper.sourceMapper().names().indexName());
        if (sourceField != null) {
            source = documentMapper.sourceMapper().nativeValue(sourceField);
            doc.removeField(documentMapper.sourceMapper().names().indexName());
        }
        return source;
    }

    private UidField.DocIdAndVersion loadCurrentVersionFromIndex(BloomCache bloomCache, Engine.Searcher searcher, Term uid) {
        UnicodeUtil.UTF8Result utf8 = Unicode.fromStringAsUtf8(uid.text());
        for (IndexReader reader : searcher.searcher().subReaders()) {
            BloomFilter filter = bloomCache.filter(reader, UidFieldMapper.NAME, true);
            // we know that its not there...
            if (!filter.isPresent(utf8.result, 0, utf8.length)) {
                continue;
            }
            UidField.DocIdAndVersion docIdAndVersion = UidField.loadDocIdAndVersion(reader, uid);
            // not null if it exists
            if (docIdAndVersion != null) {
                return docIdAndVersion;
            }
        }
        return null;
    }

    @Override protected GetRequest newRequest() {
        return new GetRequest();
    }

    @Override protected GetResponse newResponse() {
        return new GetResponse();
    }
}
