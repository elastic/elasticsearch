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

package org.elasticsearch.index.get;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.document.ResetFieldSelector;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.selector.FieldMappersFieldSelector;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.*;

/**
 */
public class ShardGetService extends AbstractIndexShardComponent {

    private final ScriptService scriptService;

    private final MapperService mapperService;

    private final IndexCache indexCache;

    private IndexShard indexShard;

    private final MeanMetric existsMetric = new MeanMetric();
    private final MeanMetric missingMetric = new MeanMetric();

    @Inject public ShardGetService(ShardId shardId, @IndexSettings Settings indexSettings, ScriptService scriptService,
                                   MapperService mapperService, IndexCache indexCache) {
        super(shardId, indexSettings);
        this.scriptService = scriptService;
        this.mapperService = mapperService;
        this.indexCache = indexCache;
    }

    public GetStats stats() {
        return new GetStats(existsMetric.count(), existsMetric.sum(), missingMetric.count(), missingMetric.sum());
    }

    // sadly, to overcome cyclic dep, we need to do this and inject it ourselves...
    public ShardGetService setIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
        return this;
    }

    public GetResult get(String type, String id, String[] gFields, boolean realtime) throws ElasticSearchException {
        long now = System.nanoTime();
        GetResult getResult = innerGet(type, id, gFields, realtime);
        if (getResult.exists()) {
            existsMetric.inc(System.nanoTime() - now);
        } else {
            missingMetric.inc(System.nanoTime() - now);
        }
        return getResult;
    }

    public GetResult innerGet(String type, String id, String[] gFields, boolean realtime) throws ElasticSearchException {
        boolean loadSource = gFields == null || gFields.length > 0;
        Engine.GetResult get = null;
        if (type == null || type.equals("_all")) {
            for (String typeX : mapperService.types()) {
                get = indexShard.get(new Engine.Get(realtime, UidFieldMapper.TERM_FACTORY.createTerm(Uid.createUid(typeX, id))).loadSource(loadSource));
                if (get.exists()) {
                    type = typeX;
                    break;
                } else {
                    get.release();
                }
            }
            if (get == null) {
                return new GetResult(shardId.index().name(), type, id, -1, false, null, null);
            }
            if (!get.exists()) {
                // no need to release here as well..., we release in the for loop for non exists
                return new GetResult(shardId.index().name(), type, id, -1, false, null, null);
            }
        } else {
            get = indexShard.get(new Engine.Get(realtime, UidFieldMapper.TERM_FACTORY.createTerm(Uid.createUid(type, id))).loadSource(loadSource));
            if (!get.exists()) {
                get.release();
                return new GetResult(shardId.index().name(), type, id, -1, false, null, null);
            }
        }

        DocumentMapper docMapper = mapperService.documentMapper(type);
        if (docMapper == null) {
            get.release();
            return new GetResult(shardId.index().name(), type, id, -1, false, null, null);
        }

        try {
            // break between having loaded it from translog (so we only have _source), and having a document to load
            if (get.docIdAndVersion() != null) {
                Map<String, GetField> fields = null;
                byte[] source = null;
                UidField.DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
                ResetFieldSelector fieldSelector = buildFieldSelectors(docMapper, gFields);
                if (fieldSelector != null) {
                    fieldSelector.reset();
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
                                searchLookup = new SearchLookup(mapperService, indexCache.fieldData());
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

                return new GetResult(shardId.index().name(), type, id, get.version(), get.exists(), source == null ? null : new BytesHolder(source), fields);
            } else {
                Translog.Source source = get.source();

                Map<String, GetField> fields = null;
                boolean sourceRequested = false;

                // we can only load scripts that can run against the source
                if (gFields == null) {
                    sourceRequested = true;
                } else if (gFields.length == 0) {
                    // no fields, and no source
                    sourceRequested = false;
                } else {
                    Map<String, Object> sourceAsMap = null;
                    SearchLookup searchLookup = null;
                    for (String field : gFields) {
                        if (field.equals("_source")) {
                            sourceRequested = true;
                            continue;
                        }
                        Object value = null;
                        if (field.equals(RoutingFieldMapper.NAME) && docMapper.routingFieldMapper().stored()) {
                            value = source.routing;
                        } else if (field.equals(ParentFieldMapper.NAME) && docMapper.parentFieldMapper() != null && docMapper.parentFieldMapper().stored()) {
                            value = source.parent;
                        } else if (field.equals(TimestampFieldMapper.NAME) && docMapper.timestampFieldMapper().stored()) {
                            value = source.timestamp;
                        } else if (field.equals(TTLFieldMapper.NAME) && docMapper.TTLFieldMapper().stored()) {
                            // Call value for search with timestamp + ttl here to display the live remaining ttl value and be consistent with the search result display
                            if (source.ttl > 0) {
                                value = docMapper.TTLFieldMapper().valueForSearch(source.timestamp + source.ttl);
                            }
                        } else {
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
                                    searchLookup = new SearchLookup(mapperService, indexCache.fieldData());
                                }
                                if (sourceAsMap == null) {
                                    sourceAsMap = SourceLookup.sourceAsMap(source.source.bytes(), source.source.offset(), source.source.length());
                                }
                                SearchScript searchScript = scriptService.search(searchLookup, "mvel", script, null);
                                // we can't do this, only allow to run scripts against the source
                                //searchScript.setNextReader(docIdAndVersion.reader);
                                //searchScript.setNextDocId(docIdAndVersion.docId);

                                // but, we need to inject the parsed source into the script, so it will be used...
                                searchScript.setNextSource(sourceAsMap);

                                try {
                                    value = searchScript.run();
                                } catch (RuntimeException e) {
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("failed to execute get request script field [{}]", e, script);
                                    }
                                    // ignore
                                }
                            }
                        }
                        if (value != null) {
                            if (fields == null) {
                                fields = newHashMapWithExpectedSize(2);
                            }
                            GetField getField = fields.get(field);
                            if (getField == null) {
                                getField = new GetField(field, new ArrayList<Object>(2));
                                fields.put(field, getField);
                            }
                            getField.values().add(value);
                        }
                    }
                }

                return new GetResult(shardId.index().name(), type, id, get.version(), get.exists(), sourceRequested ? source.source : null, fields);
            }
        } finally {
            get.release();
        }
    }

    private static ResetFieldSelector buildFieldSelectors(DocumentMapper docMapper, String... fields) {
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
}
