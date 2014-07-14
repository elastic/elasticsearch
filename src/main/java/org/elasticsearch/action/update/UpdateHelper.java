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

package org.elasticsearch.action.update;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMapWithExpectedSize;

/**
 * Helper for translating an update request to an index, delete request or update response.
 */
public class UpdateHelper extends AbstractComponent {

    private final IndicesService indicesService;
    private final ScriptService scriptService;

    @Inject
    public UpdateHelper(Settings settings, IndicesService indicesService, ScriptService scriptService) {
        super(settings);
        this.indicesService = indicesService;
        this.scriptService = scriptService;
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action).
     */
    public Result prepare(UpdateRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());
        return prepare(request, indexShard);
    }

    public Result prepare(UpdateRequest request, IndexShard indexShard) {
        long getDate = System.currentTimeMillis();
        final GetResult getResult = indexShard.getService().get(request.type(), request.id(),
                new String[]{RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME},
                true, request.version(), request.versionType(), FetchSourceContext.FETCH_SOURCE);

        if (!getResult.isExists()) {
            if (request.upsertRequest() == null && !request.docAsUpsert()) {
                throw new DocumentMissingException(new ShardId(request.index(), request.shardId()), request.type(), request.id());
            }
            IndexRequest indexRequest = request.docAsUpsert() ? request.doc() : request.upsertRequest();
            indexRequest.index(request.index()).type(request.type()).id(request.id())
                    // it has to be a "create!"
                    .create(true)
                    .routing(request.routing())
                    .refresh(request.refresh())
                    .replicationType(request.replicationType()).consistencyLevel(request.consistencyLevel());
            indexRequest.operationThreaded(false);
            if (request.versionType() != VersionType.INTERNAL) {
                // in all but the internal versioning mode, we want to create the new document using the given version.
                indexRequest.version(request.version()).versionType(request.versionType());
            }
            return new Result(indexRequest, Operation.UPSERT, null, null);
        }

        long updateVersion = getResult.getVersion();

        if (request.versionType() != VersionType.INTERNAL) {
            assert request.versionType() == VersionType.FORCE;
            updateVersion = request.version(); // remember, match_any is excluded by the conflict test
        }

        if (getResult.internalSourceRef() == null) {
            // no source, we can't do nothing, through a failure...
            throw new DocumentSourceMissingException(new ShardId(request.index(), request.shardId()), request.type(), request.id());
        }

        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        String operation = null;
        String timestamp = null;
        Long ttl = null;
        Object fetchedTTL = null;
        final Map<String, Object> updatedSourceAsMap;
        final XContentType updateSourceContentType = sourceAndContent.v1();
        String routing = getResult.getFields().containsKey(RoutingFieldMapper.NAME) ? getResult.field(RoutingFieldMapper.NAME).getValue().toString() : null;
        String parent = getResult.getFields().containsKey(ParentFieldMapper.NAME) ? getResult.field(ParentFieldMapper.NAME).getValue().toString() : null;

        if (request.script() == null && request.doc() != null) {
            IndexRequest indexRequest = request.doc();
            updatedSourceAsMap = sourceAndContent.v2();
            if (indexRequest.ttl() > 0) {
                ttl = indexRequest.ttl();
            }
            timestamp = indexRequest.timestamp();
            if (indexRequest.routing() != null) {
                routing = indexRequest.routing();
            }
            if (indexRequest.parent() != null) {
                parent = indexRequest.parent();
            }
            XContentHelper.update(updatedSourceAsMap, indexRequest.sourceAsMap());
        } else {
            Map<String, Object> ctx = new HashMap<>(2);
            ctx.put("_source", sourceAndContent.v2());

            try {
                ExecutableScript script = scriptService.executable(request.scriptLang, request.script, request.scriptType, request.scriptParams);
                script.setNextVar("ctx", ctx);
                script.run();
                // we need to unwrap the ctx...
                ctx = (Map<String, Object>) script.unwrap(ctx);
            } catch (Exception e) {
                throw new ElasticsearchIllegalArgumentException("failed to execute script", e);
            }

            operation = (String) ctx.get("op");
            timestamp = (String) ctx.get("_timestamp");

            fetchedTTL = ctx.get("_ttl");
            if (fetchedTTL != null) {
                if (fetchedTTL instanceof Number) {
                    ttl = ((Number) fetchedTTL).longValue();
                } else {
                    ttl = TimeValue.parseTimeValue((String) fetchedTTL, null).millis();
                }
            }

            updatedSourceAsMap = (Map<String, Object>) ctx.get("_source");
        }

        // apply script to update the source
        // No TTL has been given in the update script so we keep previous TTL value if there is one
        if (ttl == null) {
            ttl = getResult.getFields().containsKey(TTLFieldMapper.NAME) ? (Long) getResult.field(TTLFieldMapper.NAME).getValue() : null;
            if (ttl != null) {
                ttl = ttl - (System.currentTimeMillis() - getDate); // It is an approximation of exact TTL value, could be improved
            }
        }

        if (operation == null || "index".equals(operation)) {
            final IndexRequest indexRequest = Requests.indexRequest(request.index()).type(request.type()).id(request.id()).routing(routing).parent(parent)
                    .source(updatedSourceAsMap, updateSourceContentType)
                    .version(updateVersion).versionType(request.versionType())
                    .replicationType(request.replicationType()).consistencyLevel(request.consistencyLevel())
                    .timestamp(timestamp).ttl(ttl)
                    .refresh(request.refresh());
            indexRequest.operationThreaded(false);
            return new Result(indexRequest, Operation.INDEX, updatedSourceAsMap, updateSourceContentType);
        } else if ("delete".equals(operation)) {
            DeleteRequest deleteRequest = Requests.deleteRequest(request.index()).type(request.type()).id(request.id()).routing(routing).parent(parent)
                    .version(updateVersion).versionType(request.versionType())
                    .replicationType(request.replicationType()).consistencyLevel(request.consistencyLevel());
            deleteRequest.operationThreaded(false);
            return new Result(deleteRequest, Operation.DELETE, updatedSourceAsMap, updateSourceContentType);
        } else if ("none".equals(operation)) {
            UpdateResponse update = new UpdateResponse(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(), false);
            update.setGetResult(extractGetResult(request, getResult.getVersion(), updatedSourceAsMap, updateSourceContentType, null));
            return new Result(update, Operation.NONE, updatedSourceAsMap, updateSourceContentType);
        } else {
            logger.warn("Used update operation [{}] for script [{}], doing nothing...", operation, request.script);
            UpdateResponse update = new UpdateResponse(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(), false);
            return new Result(update, Operation.NONE, updatedSourceAsMap, updateSourceContentType);
        }
    }

    /**
     * Extracts the fields from the updated document to be returned in a update response
     */
    public GetResult extractGetResult(final UpdateRequest request, long version, final Map<String, Object> source, XContentType sourceContentType, @Nullable final BytesReference sourceAsBytes) {
        if (request.fields() == null || request.fields().length == 0) {
            return null;
        }
        boolean sourceRequested = false;
        Map<String, GetField> fields = null;
        if (request.fields() != null && request.fields().length > 0) {
            SourceLookup sourceLookup = new SourceLookup();
            sourceLookup.setNextSource(source);
            for (String field : request.fields()) {
                if (field.equals("_source")) {
                    sourceRequested = true;
                    continue;
                }
                Object value = sourceLookup.extractValue(field);
                if (value != null) {
                    if (fields == null) {
                        fields = newHashMapWithExpectedSize(2);
                    }
                    GetField getField = fields.get(field);
                    if (getField == null) {
                        getField = new GetField(field, new ArrayList<>(2));
                        fields.put(field, getField);
                    }
                    getField.getValues().add(value);
                }
            }
        }

        // TODO when using delete/none, we can still return the source as bytes by generating it (using the sourceContentType)
        return new GetResult(request.index(), request.type(), request.id(), version, true, sourceRequested ? sourceAsBytes : null, fields);
    }

    public static class Result {

        private final Streamable action;
        private final Operation operation;
        private final Map<String, Object> updatedSourceAsMap;
        private final XContentType updateSourceContentType;

        public Result(Streamable action, Operation operation, Map<String, Object> updatedSourceAsMap, XContentType updateSourceContentType) {
            this.action = action;
            this.operation = operation;
            this.updatedSourceAsMap = updatedSourceAsMap;
            this.updateSourceContentType = updateSourceContentType;
        }

        @SuppressWarnings("unchecked")
        public <T extends Streamable> T action() {
            return (T) action;
        }

        public Operation operation() {
            return operation;
        }

        public Map<String, Object> updatedSourceAsMap() {
            return updatedSourceAsMap;
        }

        public XContentType updateSourceContentType() {
            return updateSourceContentType;
        }
    }

    public static enum Operation {

        UPSERT,
        INDEX,
        DELETE,
        NONE

    }

}
