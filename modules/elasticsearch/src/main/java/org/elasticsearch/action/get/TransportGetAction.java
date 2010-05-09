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
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.single.TransportSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.lucene.Lucene;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.util.collect.Maps.*;

/**
 * Performs the get operation.
 *
 * @author kimchy (shay.banon)
 */
public class TransportGetAction extends TransportSingleOperationAction<GetRequest, GetResponse> {

    @Inject public TransportGetAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ThreadPool threadPool) {
        super(settings, threadPool, clusterService, transportService, indicesService);
    }

    @Override protected String transportAction() {
        return TransportActions.GET;
    }

    @Override protected String transportShardAction() {
        return "indices/get/shard";
    }

    @Override protected GetResponse shardOperation(GetRequest request, int shardId) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);

        DocumentMapper docMapper = indexService.mapperService().type(request.type());
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + request.type() + "]");
        }

        Engine.Searcher searcher = indexShard.searcher();
        boolean exists = false;
        byte[] source = null;
        Map<String, GetField> fields = null;
        try {
            int docId = Lucene.docId(searcher.reader(), docMapper.uidMapper().term(request.type(), request.id()));
            if (docId != Lucene.NO_DOC) {
                exists = true;
                FieldSelector fieldSelector = buildFieldSelectors(docMapper, request.fields());
                if (fieldSelector != null) {
                    Document doc = searcher.reader().document(docId, fieldSelector);
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
            }
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get type [" + request.type() + "] and id [" + request.id() + "]", e);
        } finally {
            searcher.release();
        }
        return new GetResponse(request.index(), request.type(), request.id(), exists, source, fields);
    }

    private FieldSelector buildFieldSelectors(DocumentMapper docMapper, String... fields) {
        if (fields == null) {
            return docMapper.sourceMapper().fieldSelector();
        }

        // don't load anything
        if (fields.length == 0) {
            return null;
        }

        FieldMappersFieldSelector fieldSelector = new FieldMappersFieldSelector();
        for (String fieldName : fields) {
            FieldMappers x = docMapper.mappers().smartName(fieldName);
            if (x == null) {
                throw new ElasticSearchException("No mapping for field [" + fieldName + "] in type [" + docMapper.type() + "]");
            }
            fieldSelector.add(x);
        }
        return fieldSelector;
    }

    private byte[] extractSource(Document doc, DocumentMapper documentMapper) {
        byte[] source = null;
        Fieldable sourceField = doc.getFieldable(documentMapper.sourceMapper().names().indexName());
        if (sourceField != null) {
            source = documentMapper.sourceMapper().value(sourceField);
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
