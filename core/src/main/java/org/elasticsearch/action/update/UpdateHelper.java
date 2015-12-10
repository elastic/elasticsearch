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
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper for translating an update request to an index, delete request or update response.
 */
public class UpdateHelper extends AbstractComponent {

    private final ScriptService scriptService;

    @Inject
    public UpdateHelper(Settings settings, ScriptService scriptService) {
        super(settings);
        this.scriptService = scriptService;
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action).
     */
    @SuppressWarnings("unchecked")
    public Result prepare(UpdateRequest request, IndexShard indexShard) {
        final GetResult getResult = indexShard.getService().get(request.type(), request.id(),
                new String[]{RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME, TimestampFieldMapper.NAME},
                true, request.version(), request.versionType(), FetchSourceContext.FETCH_SOURCE, false);
        return prepare(request, getResult);
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action).
     */
    @SuppressWarnings("unchecked")
    protected Result prepare(UpdateRequest request, final GetResult getResult) {
        long getDateNS = System.nanoTime();
        final ShardId shardId = new ShardId(getResult.getIndex(), request.shardId());
        if (!getResult.isExists()) {
            if (request.upsertRequest() == null && !request.docAsUpsert()) {
                throw new DocumentMissingException(shardId, request.type(), request.id());
            }
            IndexRequest indexRequest = request.docAsUpsert() ? request.doc() : request.upsertRequest();
            TimeValue ttl = indexRequest.ttl();
            if (request.scriptedUpsert() && request.script() != null) {
                // Run the script to perform the create logic
                IndexRequest upsert = request.upsertRequest();
                Map<String, Object> upsertDoc = upsert.sourceAsMap();
                Map<String, Object> ctx = new HashMap<>(2);
                // Tell the script that this is a create and not an update
                ctx.put("op", "create");
                ctx.put("_source", upsertDoc);
                ctx = executeScript(request, ctx);
                //Allow the script to set TTL using ctx._ttl
                if (ttl == null) {
                    ttl = getTTLFromScriptContext(ctx);
                }

                //Allow the script to abort the create by setting "op" to "none"
                String scriptOpChoice = (String) ctx.get("op");

                // Only valid options for an upsert script are "create"
                // (the default) or "none", meaning abort upsert
                if (!"create".equals(scriptOpChoice)) {
                    if (!"none".equals(scriptOpChoice)) {
                        logger.warn("Used upsert operation [{}] for script [{}], doing nothing...", scriptOpChoice,
                                request.script.getScript());
                    }
                    UpdateResponse update = new UpdateResponse(shardId, getResult.getType(), getResult.getId(),
                            getResult.getVersion(), false);
                    update.setGetResult(getResult);
                    return new Result(update, Operation.NONE, upsertDoc, XContentType.JSON);
                }
                indexRequest.source((Map) ctx.get("_source"));
            }

            indexRequest.index(request.index()).type(request.type()).id(request.id())
                    // it has to be a "create!"
                    .create(true)
                    .ttl(ttl)
                    .refresh(request.refresh())
                    .routing(request.routing())
                    .parent(request.parent())
                    .consistencyLevel(request.consistencyLevel());
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
            throw new DocumentSourceMissingException(shardId, request.type(), request.id());
        }

        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        String operation = null;
        String timestamp = null;
        TimeValue ttl = null;
        final Map<String, Object> updatedSourceAsMap;
        final XContentType updateSourceContentType = sourceAndContent.v1();
        String routing = getResult.getFields().containsKey(RoutingFieldMapper.NAME) ? getResult.field(RoutingFieldMapper.NAME).getValue().toString() : null;
        String parent = getResult.getFields().containsKey(ParentFieldMapper.NAME) ? getResult.field(ParentFieldMapper.NAME).getValue().toString() : null;

        if (request.script() == null && request.doc() != null) {
            IndexRequest indexRequest = request.doc();
            updatedSourceAsMap = sourceAndContent.v2();
            if (indexRequest.ttl() != null) {
                ttl = indexRequest.ttl();
            }
            timestamp = indexRequest.timestamp();
            if (indexRequest.routing() != null) {
                routing = indexRequest.routing();
            }
            if (indexRequest.parent() != null) {
                parent = indexRequest.parent();
            }
            boolean noop = !XContentHelper.update(updatedSourceAsMap, indexRequest.sourceAsMap(), request.detectNoop());
            // noop could still be true even if detectNoop isn't because update detects empty maps as noops.  BUT we can only
            // actually turn the update into a noop if detectNoop is true to preserve backwards compatibility and to handle
            // cases where users repopulating multi-fields or adding synonyms, etc.
            if (request.detectNoop() && noop) {
                operation = "none";
            }
        } else {
            Map<String, Object> ctx = new HashMap<>(16);
            Long originalTtl = getResult.getFields().containsKey(TTLFieldMapper.NAME) ? (Long) getResult.field(TTLFieldMapper.NAME).getValue() : null;
            Long originalTimestamp = getResult.getFields().containsKey(TimestampFieldMapper.NAME) ? (Long) getResult.field(TimestampFieldMapper.NAME).getValue() : null;
            ctx.put("_index", getResult.getIndex());
            ctx.put("_type", getResult.getType());
            ctx.put("_id", getResult.getId());
            ctx.put("_version", getResult.getVersion());
            ctx.put("_routing", routing);
            ctx.put("_parent", parent);
            ctx.put("_timestamp", originalTimestamp);
            ctx.put("_ttl", originalTtl);
            ctx.put("_source", sourceAndContent.v2());

            ctx = executeScript(request, ctx);

            operation = (String) ctx.get("op");

            Object fetchedTimestamp = ctx.get("_timestamp");
            if (fetchedTimestamp != null) {
                timestamp = fetchedTimestamp.toString();
            } else if (originalTimestamp != null) {
                // No timestamp has been given in the update script, so we keep the previous timestamp if there is one
                timestamp = originalTimestamp.toString();
            }

            ttl = getTTLFromScriptContext(ctx);

            updatedSourceAsMap = (Map<String, Object>) ctx.get("_source");
        }

        // apply script to update the source
        // No TTL has been given in the update script so we keep previous TTL value if there is one
        if (ttl == null) {
            Long ttlAsLong = getResult.getFields().containsKey(TTLFieldMapper.NAME) ? (Long) getResult.field(TTLFieldMapper.NAME).getValue() : null;
            if (ttlAsLong != null) {
                ttl = new TimeValue(ttlAsLong - TimeValue.nsecToMSec(System.nanoTime() - getDateNS));// It is an approximation of exact TTL value, could be improved
            }
        }

        if (operation == null || "index".equals(operation)) {
            final IndexRequest indexRequest = Requests.indexRequest(request.index()).type(request.type()).id(request.id()).routing(routing).parent(parent)
                    .source(updatedSourceAsMap, updateSourceContentType)
                    .version(updateVersion).versionType(request.versionType())
                    .consistencyLevel(request.consistencyLevel())
                    .timestamp(timestamp).ttl(ttl)
                    .refresh(request.refresh());
            return new Result(indexRequest, Operation.INDEX, updatedSourceAsMap, updateSourceContentType);
        } else if ("delete".equals(operation)) {
            DeleteRequest deleteRequest = Requests.deleteRequest(request.index()).type(request.type()).id(request.id()).routing(routing).parent(parent)
                    .version(updateVersion).versionType(request.versionType())
                    .consistencyLevel(request.consistencyLevel());
            return new Result(deleteRequest, Operation.DELETE, updatedSourceAsMap, updateSourceContentType);
        } else if ("none".equals(operation)) {
            UpdateResponse update = new UpdateResponse(shardId, getResult.getType(), getResult.getId(), getResult.getVersion(), false);
            update.setGetResult(extractGetResult(request, request.index(), getResult.getVersion(), updatedSourceAsMap, updateSourceContentType, getResult.internalSourceRef()));
            return new Result(update, Operation.NONE, updatedSourceAsMap, updateSourceContentType);
        } else {
            logger.warn("Used update operation [{}] for script [{}], doing nothing...", operation, request.script.getScript());
            UpdateResponse update = new UpdateResponse(shardId, getResult.getType(), getResult.getId(), getResult.getVersion(), false);
            return new Result(update, Operation.NONE, updatedSourceAsMap, updateSourceContentType);
        }
    }

    private Map<String, Object> executeScript(UpdateRequest request, Map<String, Object> ctx) {
        try {
            if (scriptService != null) {
                ExecutableScript script = scriptService.executable(request.script, ScriptContext.Standard.UPDATE, request);
                script.setNextVar("ctx", ctx);
                script.run();
                // we need to unwrap the ctx...
                ctx = (Map<String, Object>) script.unwrap(ctx);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to execute script", e);
        }
        return ctx;
    }

    private TimeValue getTTLFromScriptContext(Map<String, Object> ctx) {
        Object fetchedTTL = ctx.get("_ttl");
        if (fetchedTTL != null) {
            if (fetchedTTL instanceof Number) {
                return new TimeValue(((Number) fetchedTTL).longValue());
            }
            return TimeValue.parseTimeValue((String) fetchedTTL, null, "_ttl");
        }
        return null;
    }

    /**
     * Extracts the fields from the updated document to be returned in a update response
     */
    public GetResult extractGetResult(final UpdateRequest request, String concreteIndex, long version, final Map<String, Object> source, XContentType sourceContentType, @Nullable final BytesReference sourceAsBytes) {
        if (request.fields() == null || request.fields().length == 0) {
            return null;
        }
        boolean sourceRequested = false;
        Map<String, GetField> fields = null;
        if (request.fields() != null && request.fields().length > 0) {
            SourceLookup sourceLookup = new SourceLookup();
            sourceLookup.setSource(source);
            for (String field : request.fields()) {
                if (field.equals("_source")) {
                    sourceRequested = true;
                    continue;
                }
                Object value = sourceLookup.extractValue(field);
                if (value != null) {
                    if (fields == null) {
                        fields = new HashMap<>(2);
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
        return new GetResult(concreteIndex, request.type(), request.id(), version, true, sourceRequested ? sourceAsBytes : null, fields);
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

    public enum Operation {
        UPSERT,
        INDEX,
        DELETE,
        NONE
    }
}
