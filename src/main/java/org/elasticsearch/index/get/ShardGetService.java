/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.fieldvisitor.JustSourceFieldsVisitor;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.*;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMapWithExpectedSize;

/**
 */
public class ShardGetService extends AbstractIndexShardComponent {

    private final ScriptService scriptService;

    private final MapperService mapperService;

    private final IndexFieldDataService fieldDataService;

    private IndexShard indexShard;

    private final MeanMetric existsMetric = new MeanMetric();
    private final MeanMetric missingMetric = new MeanMetric();
    private final CounterMetric currentMetric = new CounterMetric();

    @Inject
    public ShardGetService(ShardId shardId, @IndexSettings Settings indexSettings, ScriptService scriptService,
                           MapperService mapperService, IndexFieldDataService fieldDataService) {
        super(shardId, indexSettings);
        this.scriptService = scriptService;
        this.mapperService = mapperService;
        this.fieldDataService = fieldDataService;
    }

    public GetStats stats() {
        return new GetStats(existsMetric.count(), TimeUnit.NANOSECONDS.toMillis(existsMetric.sum()), missingMetric.count(), TimeUnit.NANOSECONDS.toMillis(missingMetric.sum()), currentMetric.count());
    }

    // sadly, to overcome cyclic dep, we need to do this and inject it ourselves...
    public ShardGetService setIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
        return this;
    }

    public GetResult get(String type, String id, String[] gFields, boolean realtime) throws ElasticSearchException {
        currentMetric.inc();
        try {
            long now = System.nanoTime();
            GetResult getResult = innerGet(type, id, gFields, realtime);
            if (getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now);
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    /**
     * Returns {@link GetResult} based on the specified {@link Engine.GetResult} argument.
     * This method basically loads specified fields for the associated document in the engineGetResult.
     * This method load the fields from the Lucene index and not from transaction log and therefore isn't realtime.
     * <p/>
     * Note: Call <b>must</b> release engine searcher associated with engineGetResult!
     */
    public GetResult get(Engine.GetResult engineGetResult, String id, String type, String[] fields) {
        if (!engineGetResult.exists()) {
            return new GetResult(shardId.index().name(), type, id, -1, false, null, null);
        }

        currentMetric.inc();
        try {
            long now = System.nanoTime();
            DocumentMapper docMapper = mapperService.documentMapper(type);
            if (docMapper == null) {
                missingMetric.inc(System.nanoTime() - now);
                return new GetResult(shardId.index().name(), type, id, -1, false, null, null);
            }

            GetResult getResult = innerGetLoadFromStoredFields(type, id, fields, engineGetResult, docMapper);
            if (getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now); // This shouldn't happen...
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    public GetResult innerGet(String type, String id, String[] gFields, boolean realtime) throws ElasticSearchException {
        boolean loadSource = gFields == null || gFields.length > 0;
        Engine.GetResult get = null;
        if (type == null || type.equals("_all")) {
            for (String typeX : mapperService.types()) {
                get = indexShard.get(new Engine.Get(realtime, new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(typeX, id))).loadSource(loadSource));
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
            get = indexShard.get(new Engine.Get(realtime, new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(type, id))).loadSource(loadSource));
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
                return innerGetLoadFromStoredFields(type, id, gFields, get, docMapper);
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
                        if (field.equals(RoutingFieldMapper.NAME) && docMapper.routingFieldMapper().fieldType().stored()) {
                            value = source.routing;
                        } else if (field.equals(ParentFieldMapper.NAME) && docMapper.parentFieldMapper().active() && docMapper.parentFieldMapper().fieldType().stored()) {
                            value = source.parent;
                        } else if (field.equals(TimestampFieldMapper.NAME) && docMapper.timestampFieldMapper().fieldType().stored()) {
                            value = source.timestamp;
                        } else if (field.equals(TTLFieldMapper.NAME) && docMapper.TTLFieldMapper().fieldType().stored()) {
                            // Call value for search with timestamp + ttl here to display the live remaining ttl value and be consistent with the search result display
                            if (source.ttl > 0) {
                                value = docMapper.TTLFieldMapper().valueForSearch(source.timestamp + source.ttl);
                            }
                        } else if (field.equals(SizeFieldMapper.NAME) && docMapper.rootMapper(SizeFieldMapper.class).fieldType().stored()) {
                            value = source.source.length();
                        } else {
                            if (field.contains("_source.")) {
                                if (searchLookup == null) {
                                    searchLookup = new SearchLookup(mapperService, fieldDataService, new String[]{type});
                                }
                                if (sourceAsMap == null) {
                                    sourceAsMap = SourceLookup.sourceAsMap(source.source);
                                }
                                SearchScript searchScript = scriptService.search(searchLookup, "mvel", field, null);
                                // we can't do this, only allow to run scripts against the source
                                //searchScript.setNextReader(docIdAndVersion.reader);
                                //searchScript.setNextDocId(docIdAndVersion.docId);

                                // but, we need to inject the parsed source into the script, so it will be used...
                                searchScript.setNextSource(sourceAsMap);

                                try {
                                    value = searchScript.run();
                                } catch (RuntimeException e) {
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("failed to execute get request script field [{}]", e, field);
                                    }
                                    // ignore
                                }
                            } else {
                                if (searchLookup == null) {
                                    searchLookup = new SearchLookup(mapperService, fieldDataService, new String[]{type});
                                    searchLookup.source().setNextSource(source.source);
                                }

                                FieldMapper<?> x = docMapper.mappers().smartNameFieldMapper(field);
                                // only if the field is stored or source is enabled we should add it..
                                if (docMapper.sourceMapper().enabled() || x == null || x.fieldType().stored()) {
                                    value = searchLookup.source().extractValue(field);
                                    // normalize the data if needed (mainly for binary fields, to convert from base64 strings to bytes)
                                    if (value != null && x != null) {
                                        if (value instanceof List) {
                                            List list = (List) value;
                                            for (int i = 0; i < list.size(); i++) {
                                                list.set(i, x.valueForSearch(list.get(i)));
                                            }
                                        } else {
                                            value = x.valueForSearch(value);
                                        }
                                    }
                                }
                            }
                        }
                        if (value != null) {
                            if (fields == null) {
                                fields = newHashMapWithExpectedSize(2);
                            }
                            if (value instanceof List) {
                                fields.put(field, new GetField(field, (List) value));
                            } else {
                                fields.put(field, new GetField(field, ImmutableList.of(value)));
                            }
                        }
                    }
                }

                // if source is not enabled, don't return it even though we have it from the translog
                if (sourceRequested && !docMapper.sourceMapper().enabled()) {
                    sourceRequested = false;
                }

                // Cater for source excludes/includes at the cost of performance
                BytesReference sourceToBeReturned = null;
                if (sourceRequested) {
                    sourceToBeReturned = source.source;

                    SourceFieldMapper sourceFieldMapper = docMapper.sourceMapper();
                    if (sourceFieldMapper.enabled()) {
                        boolean filtered = sourceFieldMapper.includes().length > 0 || sourceFieldMapper.excludes().length > 0;
                        if (filtered) {
                            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source.source, true);
                            Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), sourceFieldMapper.includes(), sourceFieldMapper.excludes());
                            try {
                                sourceToBeReturned = XContentFactory.contentBuilder(mapTuple.v1()).map(filteredSource).bytes();
                            } catch (IOException e) {
                                throw new ElasticSearchException("Failed to get type [" + type + "] and id [" + id + "] with includes/excludes set", e);
                            }
                        }
                    }
                }

                return new GetResult(shardId.index().name(), type, id, get.version(), get.exists(), sourceToBeReturned, fields);
            }
        } finally {
            get.release();
        }
    }

    private GetResult innerGetLoadFromStoredFields(String type, String id, String[] gFields, Engine.GetResult get, DocumentMapper docMapper) {
        Map<String, GetField> fields = null;
        BytesReference source = null;
        UidField.DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
        FieldsVisitor fieldVisitor = buildFieldsVisitors(gFields);
        if (fieldVisitor != null) {
            try {
                docIdAndVersion.reader.reader().document(docIdAndVersion.docId, fieldVisitor);
            } catch (IOException e) {
                throw new ElasticSearchException("Failed to get type [" + type + "] and id [" + id + "]", e);
            }
            source = fieldVisitor.source();

            if (!fieldVisitor.fields().isEmpty()) {
                fieldVisitor.postProcess(docMapper);
                fields = new HashMap<String, GetField>(fieldVisitor.fields().size());
                for (Map.Entry<String, List<Object>> entry : fieldVisitor.fields().entrySet()) {
                    fields.put(entry.getKey(), new GetField(entry.getKey(), entry.getValue()));
                }
            }
        }

        // now, go and do the script thingy if needed
        if (gFields != null && gFields.length > 0) {
            SearchLookup searchLookup = null;
            for (String field : gFields) {
                Object value = null;
                if (field.contains("_source.") || field.contains("doc[")) {
                    if (searchLookup == null) {
                        searchLookup = new SearchLookup(mapperService, fieldDataService, new String[]{type});
                    }
                    SearchScript searchScript = scriptService.search(searchLookup, "mvel", field, null);
                    searchScript.setNextReader(docIdAndVersion.reader);
                    searchScript.setNextDocId(docIdAndVersion.docId);

                    try {
                        value = searchScript.run();
                    } catch (RuntimeException e) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("failed to execute get request script field [{}]", e, field);
                        }
                        // ignore
                    }
                } else {
                    FieldMappers x = docMapper.mappers().smartName(field);
                    if (x == null || !x.mapper().fieldType().stored()) {
                        if (searchLookup == null) {
                            searchLookup = new SearchLookup(mapperService, fieldDataService, new String[]{type});
                            searchLookup.setNextReader(docIdAndVersion.reader);
                            searchLookup.setNextDocId(docIdAndVersion.docId);
                        }
                        value = searchLookup.source().extractValue(field);
                        // normalize the data if needed (mainly for binary fields, to convert from base64 strings to bytes)
                        if (value != null && x != null) {
                            if (value instanceof List) {
                                List list = (List) value;
                                for (int i = 0; i < list.size(); i++) {
                                    list.set(i, x.mapper().valueForSearch(list.get(i)));
                                }
                            } else {
                                value = x.mapper().valueForSearch(value);
                            }
                        }
                    }
                }

                if (value != null) {
                    if (fields == null) {
                        fields = newHashMapWithExpectedSize(2);
                    }
                    if (value instanceof List) {
                        fields.put(field, new GetField(field, (List) value));
                    } else {
                        fields.put(field, new GetField(field, ImmutableList.of(value)));
                    }
                }
            }
        }

        return new GetResult(shardId.index().name(), type, id, get.version(), get.exists(), source, fields);
    }

    private static FieldsVisitor buildFieldsVisitors(String... fields) {
        if (fields == null) {
            return new JustSourceFieldsVisitor();
        }

        // don't load anything
        if (fields.length == 0) {
            return null;
        }

        return new CustomFieldsVisitor(Sets.newHashSet(fields), false);
    }
}
