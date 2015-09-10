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

package org.elasticsearch.index.get;

import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 */
public final class ShardGetService extends AbstractIndexShardComponent {
    private final MapperService mapperService;
    private final MeanMetric existsMetric = new MeanMetric();
    private final MeanMetric missingMetric = new MeanMetric();
    private final CounterMetric currentMetric = new CounterMetric();
    private final IndexShard indexShard;

    public ShardGetService(IndexShard indexShard,
                           MapperService mapperService) {
        super(indexShard.shardId(), indexShard.indexSettings());
        this.mapperService = mapperService;
        this.indexShard = indexShard;
    }

    public GetStats stats() {
        return new GetStats(existsMetric.count(), TimeUnit.NANOSECONDS.toMillis(existsMetric.sum()), missingMetric.count(), TimeUnit.NANOSECONDS.toMillis(missingMetric.sum()), currentMetric.count());
    }


    public GetResult get(String type, String id, String[] gFields, boolean realtime, long version, VersionType versionType, FetchSourceContext fetchSourceContext, boolean ignoreErrorsOnGeneratedFields) {
        currentMetric.inc();
        try {
            long now = System.nanoTime();
            GetResult getResult = innerGet(type, id, gFields, realtime, version, versionType, fetchSourceContext, ignoreErrorsOnGeneratedFields);

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
    public GetResult get(Engine.GetResult engineGetResult, String id, String type, String[] fields, FetchSourceContext fetchSourceContext, boolean ignoreErrorsOnGeneratedFields) {
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
            fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, fields);
            GetResult getResult = innerGetLoadFromStoredFields(type, id, fields, fetchSourceContext, engineGetResult, docMapper, ignoreErrorsOnGeneratedFields);
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

    /**
     * decides what needs to be done based on the request input and always returns a valid non-null FetchSourceContext
     */
    private FetchSourceContext normalizeFetchSourceContent(@Nullable FetchSourceContext context, @Nullable String[] gFields) {
        if (context != null) {
            return context;
        }
        if (gFields == null) {
            return FetchSourceContext.FETCH_SOURCE;
        }
        for (String field : gFields) {
            if (SourceFieldMapper.NAME.equals(field)) {
                return FetchSourceContext.FETCH_SOURCE;
            }
        }
        return FetchSourceContext.DO_NOT_FETCH_SOURCE;
    }

    private GetResult innerGet(String type, String id, String[] gFields, boolean realtime, long version, VersionType versionType, FetchSourceContext fetchSourceContext, boolean ignoreErrorsOnGeneratedFields) {
        fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, gFields);

        Engine.GetResult get = null;
        if (type == null || type.equals("_all")) {
            for (String typeX : mapperService.types()) {
                get = indexShard.get(new Engine.Get(realtime, new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(typeX, id)))
                        .version(version).versionType(versionType));
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
            get = indexShard.get(new Engine.Get(realtime, new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(type, id)))
                    .version(version).versionType(versionType));
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
                return innerGetLoadFromStoredFields(type, id, gFields, fetchSourceContext, get, docMapper, ignoreErrorsOnGeneratedFields);
            } else {
                Translog.Source source = get.source();

                Map<String, GetField> fields = null;
                SearchLookup searchLookup = null;

                // we can only load scripts that can run against the source
                Set<String> neededFields = new HashSet<>();
                // add meta fields
                neededFields.add(RoutingFieldMapper.NAME);
                if (docMapper.parentFieldMapper().active()) {
                    neededFields.add(ParentFieldMapper.NAME);
                }
                if (docMapper.timestampFieldMapper().enabled()) {
                    neededFields.add(TimestampFieldMapper.NAME);
                }
                if (docMapper.TTLFieldMapper().enabled()) {
                    neededFields.add(TTLFieldMapper.NAME);
                }
                // add requested fields
                if (gFields != null) {
                    neededFields.addAll(Arrays.asList(gFields));
                }
                for (String field : neededFields) {
                    if (SourceFieldMapper.NAME.equals(field)) {
                        // dealt with when normalizing fetchSourceContext.
                        continue;
                    }
                    Object value = null;
                    if (field.equals(RoutingFieldMapper.NAME)) {
                        value = source.routing;
                    } else if (field.equals(ParentFieldMapper.NAME) && docMapper.parentFieldMapper().active()) {
                        value = source.parent;
                    } else if (field.equals(TimestampFieldMapper.NAME) && docMapper.timestampFieldMapper().enabled()) {
                        value = source.timestamp;
                    } else if (field.equals(TTLFieldMapper.NAME) && docMapper.TTLFieldMapper().enabled()) {
                        // Call value for search with timestamp + ttl here to display the live remaining ttl value and be consistent with the search result display
                        if (source.ttl > 0) {
                            value = docMapper.TTLFieldMapper().valueForSearch(source.timestamp + source.ttl);
                        }
                    } else {
                        if (searchLookup == null) {
                            searchLookup = new SearchLookup(mapperService, null, new String[]{type});
                            searchLookup.source().setSource(source.source);
                        }

                        FieldMapper fieldMapper = docMapper.mappers().smartNameFieldMapper(field);
                        if (fieldMapper == null) {
                            if (docMapper.objectMappers().get(field) != null) {
                                // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                                throw new IllegalArgumentException("field [" + field + "] isn't a leaf field");
                            }
                        } else if (shouldGetFromSource(ignoreErrorsOnGeneratedFields, docMapper, fieldMapper)) {
                            List<Object> values = searchLookup.source().extractRawValues(field);
                            if (!values.isEmpty()) {
                                for (int i = 0; i < values.size(); i++) {
                                    values.set(i, fieldMapper.fieldType().valueForSearch(values.get(i)));
                                }
                                value = values;
                            }

                        }
                    }
                    if (value != null) {
                        if (fields == null) {
                            fields = new HashMap<>(2);
                        }
                        if (value instanceof List) {
                            fields.put(field, new GetField(field, (List) value));
                        } else {
                            fields.put(field, new GetField(field, Collections.singletonList(value)));
                        }
                    }
                }

                // deal with source, but only if it's enabled (we always have it from the translog)
                BytesReference sourceToBeReturned = null;
                SourceFieldMapper sourceFieldMapper = docMapper.sourceMapper();
                if (fetchSourceContext.fetchSource() && sourceFieldMapper.enabled()) {

                    sourceToBeReturned = source.source;

                    // Cater for source excludes/includes at the cost of performance
                    // We must first apply the field mapper filtering to make sure we get correct results
                    // in the case that the fetchSourceContext white lists something that's not included by the field mapper

                    boolean sourceFieldFiltering = sourceFieldMapper.includes().length > 0 || sourceFieldMapper.excludes().length > 0;
                    boolean sourceFetchFiltering = fetchSourceContext.includes().length > 0 || fetchSourceContext.excludes().length > 0;
                    if (fetchSourceContext.transformSource() || sourceFieldFiltering || sourceFetchFiltering) {
                        // TODO: The source might parsed and available in the sourceLookup but that one uses unordered maps so different. Do we care?
                        Tuple<XContentType, Map<String, Object>> typeMapTuple = XContentHelper.convertToMap(source.source, true);
                        XContentType sourceContentType = typeMapTuple.v1();
                        Map<String, Object> sourceAsMap = typeMapTuple.v2();
                        if (fetchSourceContext.transformSource()) {
                            sourceAsMap = docMapper.transformSourceAsMap(sourceAsMap);
                        }
                        if (sourceFieldFiltering) {
                            sourceAsMap = XContentMapValues.filter(sourceAsMap, sourceFieldMapper.includes(), sourceFieldMapper.excludes());
                        }
                        if (sourceFetchFiltering) {
                            sourceAsMap = XContentMapValues.filter(sourceAsMap, fetchSourceContext.includes(), fetchSourceContext.excludes());
                        }
                        try {
                            sourceToBeReturned = XContentFactory.contentBuilder(sourceContentType).map(sourceAsMap).bytes();
                        } catch (IOException e) {
                            throw new ElasticsearchException("Failed to get type [" + type + "] and id [" + id + "] with includes/excludes set", e);
                        }
                    }
                }

                return new GetResult(shardId.index().name(), type, id, get.version(), get.exists(), sourceToBeReturned, fields);
            }
        } finally {
            get.release();
        }
    }

    protected boolean shouldGetFromSource(boolean ignoreErrorsOnGeneratedFields, DocumentMapper docMapper, FieldMapper fieldMapper) {
        if (!fieldMapper.isGenerated()) {
            //if the field is always there we check if either source mapper is enabled, in which case we get the field
            // from source, or, if the field is stored, in which case we have to get if from source here also (we are in the translog phase, doc not indexed yet, we annot access the stored fields)
            return docMapper.sourceMapper().enabled() || fieldMapper.fieldType().stored();
        } else {
            if (!fieldMapper.fieldType().stored()) {
                //if it is not stored, user will not get the generated field back
                return false;
            } else {
                if (ignoreErrorsOnGeneratedFields) {
                    return false;
                } else {
                    throw new ElasticsearchException("Cannot access field " + fieldMapper.name() + " from transaction log. You can only get this field after refresh() has been called.");
                }
            }

        }
    }

    private GetResult innerGetLoadFromStoredFields(String type, String id, String[] gFields, FetchSourceContext fetchSourceContext, Engine.GetResult get, DocumentMapper docMapper, boolean ignoreErrorsOnGeneratedFields) {
        Map<String, GetField> fields = null;
        BytesReference source = null;
        Versions.DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
        FieldsVisitor fieldVisitor = buildFieldsVisitors(gFields, fetchSourceContext);
        if (fieldVisitor != null) {
            try {
                docIdAndVersion.context.reader().document(docIdAndVersion.docId, fieldVisitor);
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get type [" + type + "] and id [" + id + "]", e);
            }
            source = fieldVisitor.source();

            if (!fieldVisitor.fields().isEmpty()) {
                fieldVisitor.postProcess(docMapper);
                fields = new HashMap<>(fieldVisitor.fields().size());
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
                FieldMapper fieldMapper = docMapper.mappers().smartNameFieldMapper(field);
                if (fieldMapper == null) {
                    if (docMapper.objectMappers().get(field) != null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        throw new IllegalArgumentException("field [" + field + "] isn't a leaf field");
                    }
                } else if (!fieldMapper.fieldType().stored() && !fieldMapper.isGenerated()) {
                    if (searchLookup == null) {
                        searchLookup = new SearchLookup(mapperService, null, new String[]{type});
                        LeafSearchLookup leafSearchLookup = searchLookup.getLeafSearchLookup(docIdAndVersion.context);
                        searchLookup.source().setSource(source);
                        leafSearchLookup.setDocument(docIdAndVersion.docId);
                    }

                    List<Object> values = searchLookup.source().extractRawValues(field);
                    if (!values.isEmpty()) {
                        for (int i = 0; i < values.size(); i++) {
                            values.set(i, fieldMapper.fieldType().valueForSearch(values.get(i)));
                        }
                        value = values;
                    }
                }

                if (value != null) {
                    if (fields == null) {
                        fields = new HashMap<>(2);
                    }
                    if (value instanceof List) {
                        fields.put(field, new GetField(field, (List) value));
                    } else {
                        fields.put(field, new GetField(field, Collections.singletonList(value)));
                    }
                }
            }
        }

        if (!fetchSourceContext.fetchSource()) {
            source = null;
        } else if (fetchSourceContext.transformSource() || fetchSourceContext.includes().length > 0 || fetchSourceContext.excludes().length > 0) {
            Map<String, Object> sourceAsMap;
            XContentType sourceContentType = null;
            // TODO: The source might parsed and available in the sourceLookup but that one uses unordered maps so different. Do we care?
            Tuple<XContentType, Map<String, Object>> typeMapTuple = XContentHelper.convertToMap(source, true);
            sourceContentType = typeMapTuple.v1();
            sourceAsMap = typeMapTuple.v2();
            if (fetchSourceContext.transformSource()) {
                sourceAsMap = docMapper.transformSourceAsMap(sourceAsMap);
            }
            sourceAsMap = XContentMapValues.filter(sourceAsMap, fetchSourceContext.includes(), fetchSourceContext.excludes());
            try {
                source = XContentFactory.contentBuilder(sourceContentType).map(sourceAsMap).bytes();
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get type [" + type + "] and id [" + id + "] with includes/excludes set", e);
            }
        }

        return new GetResult(shardId.index().name(), type, id, get.version(), get.exists(), source, fields);
    }

    private static FieldsVisitor buildFieldsVisitors(String[] fields, FetchSourceContext fetchSourceContext) {
        if (fields == null || fields.length == 0) {
            return fetchSourceContext.fetchSource() ? new FieldsVisitor(true) : null;
        }

        return new CustomFieldsVisitor(Sets.newHashSet(fields), fetchSourceContext.fetchSource());
    }
}
