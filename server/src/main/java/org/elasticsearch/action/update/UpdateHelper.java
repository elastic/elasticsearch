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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Helper for translating an update request to an index, delete request or update response.
 */
public class UpdateHelper extends AbstractComponent {
    private final ScriptService scriptService;

    public UpdateHelper(Settings settings, ScriptService scriptService) {
        super(settings);
        this.scriptService = scriptService;
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action).
     */
    public Result prepare(UpdateRequest request, IndexShard indexShard, LongSupplier nowInMillis) {
        final GetResult getResult = indexShard.getService().get(request.type(), request.id(),
                new String[]{RoutingFieldMapper.NAME, ParentFieldMapper.NAME},
                true, request.version(), request.versionType(), FetchSourceContext.FETCH_SOURCE);
        return prepare(indexShard.shardId(), request, getResult, nowInMillis);
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action, in the event of a
     * noop).
     */
    @SuppressWarnings("unchecked")
    protected Result prepare(ShardId shardId, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
        if (getResult.isExists() == false) {
            // If the document didn't exist, execute the update request as an upsert
            return prepareUpsert(shardId, request, getResult, nowInMillis);
        } else if (getResult.internalSourceRef() == null) {
            // no source, we can't do anything, throw a failure...
            throw new DocumentSourceMissingException(shardId, request.type(), request.id());
        } else if (request.script() == null && request.doc() != null) {
            // The request has no script, it is a new doc that should be merged with the old document
            return prepareUpdateIndexRequest(shardId, request, getResult, request.detectNoop());
        } else {
            // The request has a script (or empty script), execute the script and prepare a new index request
            return prepareUpdateScriptRequest(shardId, request, getResult, nowInMillis);
        }
    }

    /**
     * Execute a scripted upsert, where there is an existing upsert document and a script to be executed. The script is executed and a new
     * Tuple of operation and updated {@code _source} is returned.
     */
    Tuple<UpdateOpType, Map<String, Object>> executeScriptedUpsert(IndexRequest upsert, Script script, LongSupplier nowInMillis) {
        Map<String, Object> upsertDoc = upsert.sourceAsMap();
        Map<String, Object> ctx = new HashMap<>(3);
        // Tell the script that this is a create and not an update
        ctx.put(ContextFields.OP, UpdateOpType.CREATE.toString());
        ctx.put(ContextFields.SOURCE, upsertDoc);
        ctx.put(ContextFields.NOW, nowInMillis.getAsLong());
        ctx = executeScript(script, ctx);

        UpdateOpType operation = UpdateOpType.lenientFromString((String) ctx.get(ContextFields.OP), logger, script.getIdOrCode());
        Map newSource = (Map) ctx.get(ContextFields.SOURCE);

        if (operation != UpdateOpType.CREATE && operation != UpdateOpType.NONE) {
            // Only valid options for an upsert script are "create" (the default) or "none", meaning abort upsert
            logger.warn("Invalid upsert operation [{}] for script [{}], doing nothing...", operation, script.getIdOrCode());
            operation = UpdateOpType.NONE;
        }

        return new Tuple<>(operation, newSource);
    }

    /**
     * Prepare the request for upsert, executing the upsert script if present, and returning a {@code Result} containing a new
     * {@code IndexRequest} to be executed on the primary and replicas.
     */
    Result prepareUpsert(ShardId shardId, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
            if (request.upsertRequest() == null && !request.docAsUpsert()) {
                throw new DocumentMissingException(shardId, request.type(), request.id());
            }
            IndexRequest indexRequest = request.docAsUpsert() ? request.doc() : request.upsertRequest();
            if (request.scriptedUpsert() && request.script() != null) {
                // Run the script to perform the create logic
                IndexRequest upsert = request.upsertRequest();
                Tuple<UpdateOpType, Map<String, Object>> upsertResult = executeScriptedUpsert(upsert, request.script, nowInMillis);
                switch (upsertResult.v1()) {
                    case CREATE:
                        // Update the index request with the new "_source"
                        indexRequest.source(upsertResult.v2());
                        break;
                    case NONE:
                        UpdateResponse update = new UpdateResponse(shardId, getResult.getType(), getResult.getId(),
                                getResult.getVersion(), DocWriteResponse.Result.NOOP);
                        update.setGetResult(getResult);
                        return new Result(update, DocWriteResponse.Result.NOOP, upsertResult.v2(), XContentType.JSON);
                    default:
                        // It's fine to throw an exception here, the leniency is handled/logged by `executeScriptedUpsert`
                        throw new IllegalArgumentException("unknown upsert operation, got: " + upsertResult.v1());
                }
            }

            indexRequest.index(request.index())
                    .type(request.type()).id(request.id()).setRefreshPolicy(request.getRefreshPolicy()).routing(request.routing())
                    .parent(request.parent()).timeout(request.timeout()).waitForActiveShards(request.waitForActiveShards())
                    // it has to be a "create!"
                    .create(true);

            if (request.versionType() != VersionType.INTERNAL) {
                // in all but the internal versioning mode, we want to create the new document using the given version.
                indexRequest.version(request.version()).versionType(request.versionType());
            }

            return new Result(indexRequest, DocWriteResponse.Result.CREATED, null, null);
    }

    /**
     * Calculate the version to use for the update request, using either the existing version if internal versioning is used, or the get
     * result document's version if the version type is "FORCE".
     */
    static long calculateUpdateVersion(UpdateRequest request, GetResult getResult) {
        if (request.versionType() != VersionType.INTERNAL) {
            assert request.versionType() == VersionType.FORCE;
            return request.version(); // remember, match_any is excluded by the conflict test
        } else {
            return getResult.getVersion();
        }
    }

    /**
     * Calculate a routing value to be used, either the included index request's routing, or retrieved document's routing when defined.
     */
    @Nullable
    static String calculateRouting(GetResult getResult, @Nullable IndexRequest updateIndexRequest) {
        if (updateIndexRequest != null && updateIndexRequest.routing() != null) {
            return updateIndexRequest.routing();
        } else if (getResult.getFields().containsKey(RoutingFieldMapper.NAME)) {
            return getResult.field(RoutingFieldMapper.NAME).getValue().toString();
        } else {
            return null;
        }
    }

    /**
     * Calculate a parent value to be used, either the included index request's parent, or retrieved document's parent when defined.
     */
    @Nullable
    static String calculateParent(GetResult getResult, @Nullable IndexRequest updateIndexRequest) {
        if (updateIndexRequest != null && updateIndexRequest.parent() != null) {
            return updateIndexRequest.parent();
        } else if (getResult.getFields().containsKey(ParentFieldMapper.NAME)) {
            return getResult.field(ParentFieldMapper.NAME).getValue().toString();
        } else {
            return null;
        }
    }

    /**
     * Prepare the request for merging the existing document with a new one, can optionally detect a noop change. Returns a {@code Result}
     * containing a new {@code IndexRequest} to be executed on the primary and replicas.
     */
    Result prepareUpdateIndexRequest(ShardId shardId, UpdateRequest request, GetResult getResult, boolean detectNoop) {
        final long updateVersion = calculateUpdateVersion(request, getResult);
        final IndexRequest currentRequest = request.doc();
        final String routing = calculateRouting(getResult, currentRequest);
        final String parent = calculateParent(getResult, currentRequest);
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> updatedSourceAsMap = sourceAndContent.v2();

        final boolean noop = !XContentHelper.update(updatedSourceAsMap, currentRequest.sourceAsMap(), detectNoop);

        // We can only actually turn the update into a noop if detectNoop is true to preserve backwards compatibility and to handle cases
        // where users repopulating multi-fields or adding synonyms, etc.
        if (detectNoop && noop) {
            UpdateResponse update = new UpdateResponse(shardId, getResult.getType(), getResult.getId(),
                    getResult.getVersion(), DocWriteResponse.Result.NOOP);
            update.setGetResult(extractGetResult(request, request.index(), getResult.getVersion(), updatedSourceAsMap,
                            updateSourceContentType, getResult.internalSourceRef()));
            return new Result(update, DocWriteResponse.Result.NOOP, updatedSourceAsMap, updateSourceContentType);
        } else {
            final IndexRequest finalIndexRequest = Requests.indexRequest(request.index())
                    .type(request.type()).id(request.id()).routing(routing).parent(parent)
                    .source(updatedSourceAsMap, updateSourceContentType).version(updateVersion).versionType(request.versionType())
                    .waitForActiveShards(request.waitForActiveShards()).timeout(request.timeout())
                    .setRefreshPolicy(request.getRefreshPolicy());
            return new Result(finalIndexRequest, DocWriteResponse.Result.UPDATED, updatedSourceAsMap, updateSourceContentType);
        }
    }

    /**
     * Prepare the request for updating an existing document using a script. Executes the script and returns a {@code Result} containing
     * either a new {@code IndexRequest} or {@code DeleteRequest} (depending on the script's returned "op" value) to be executed on the
     * primary and replicas.
     */
    Result prepareUpdateScriptRequest(ShardId shardId, UpdateRequest request, GetResult getResult, LongSupplier nowInMillis) {
        final long updateVersion = calculateUpdateVersion(request, getResult);
        final IndexRequest currentRequest = request.doc();
        final String routing = calculateRouting(getResult, currentRequest);
        final String parent = calculateParent(getResult, currentRequest);
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> sourceAsMap = sourceAndContent.v2();

        Map<String, Object> ctx = new HashMap<>(16);
        ctx.put(ContextFields.OP, UpdateOpType.INDEX.toString()); // The default operation is "index"
        ctx.put(ContextFields.INDEX, getResult.getIndex());
        ctx.put(ContextFields.TYPE, getResult.getType());
        ctx.put(ContextFields.ID, getResult.getId());
        ctx.put(ContextFields.VERSION, getResult.getVersion());
        ctx.put(ContextFields.ROUTING, routing);
        ctx.put(ContextFields.PARENT, parent);
        ctx.put(ContextFields.SOURCE, sourceAsMap);
        ctx.put(ContextFields.NOW, nowInMillis.getAsLong());

        ctx = executeScript(request.script, ctx);

        UpdateOpType operation = UpdateOpType.lenientFromString((String) ctx.get(ContextFields.OP), logger, request.script.getIdOrCode());

        final Map<String, Object> updatedSourceAsMap = (Map<String, Object>) ctx.get(ContextFields.SOURCE);

        switch (operation) {
            case INDEX:
                final IndexRequest indexRequest = Requests.indexRequest(request.index())
                        .type(request.type()).id(request.id()).routing(routing).parent(parent)
                        .source(updatedSourceAsMap, updateSourceContentType).version(updateVersion).versionType(request.versionType())
                        .waitForActiveShards(request.waitForActiveShards()).timeout(request.timeout())
                        .setRefreshPolicy(request.getRefreshPolicy());
                return new Result(indexRequest, DocWriteResponse.Result.UPDATED, updatedSourceAsMap, updateSourceContentType);
            case DELETE:
                DeleteRequest deleteRequest = Requests.deleteRequest(request.index())
                        .type(request.type()).id(request.id()).routing(routing).parent(parent)
                        .version(updateVersion).versionType(request.versionType()).waitForActiveShards(request.waitForActiveShards())
                        .timeout(request.timeout()).setRefreshPolicy(request.getRefreshPolicy());
                return new Result(deleteRequest, DocWriteResponse.Result.DELETED, updatedSourceAsMap, updateSourceContentType);
            default:
                // If it was neither an INDEX or DELETE operation, treat it as a noop
                UpdateResponse update = new UpdateResponse(shardId, getResult.getType(), getResult.getId(),
                        getResult.getVersion(), DocWriteResponse.Result.NOOP);
                update.setGetResult(extractGetResult(request, request.index(), getResult.getVersion(), updatedSourceAsMap,
                                updateSourceContentType, getResult.internalSourceRef()));
                return new Result(update, DocWriteResponse.Result.NOOP, updatedSourceAsMap, updateSourceContentType);

        }
    }

    private Map<String, Object> executeScript(Script script, Map<String, Object> ctx) {
        try {
            if (scriptService != null) {
                ExecutableScript.Factory factory = scriptService.compile(script, ExecutableScript.UPDATE_CONTEXT);
                ExecutableScript executableScript = factory.newInstance(script.getParams());
                executableScript.setNextVar(ContextFields.CTX, ctx);
                executableScript.run();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to execute script", e);
        }
        return ctx;
    }

    /**
     * Applies {@link UpdateRequest#fetchSource()} to the _source of the updated document to be returned in a update response.
     * For BWC this function also extracts the {@link UpdateRequest#fields()} from the updated document to be returned in a update response
     */
    public static GetResult extractGetResult(final UpdateRequest request, String concreteIndex, long version,
                                             final Map<String, Object> source, XContentType sourceContentType,
                                             @Nullable final BytesReference sourceAsBytes) {
        if ((request.fields() == null || request.fields().length == 0) &&
            (request.fetchSource() == null || request.fetchSource().fetchSource() == false)) {
            return null;
        }
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(source);
        boolean sourceRequested = false;
        Map<String, DocumentField> fields = null;
        if (request.fields() != null && request.fields().length > 0) {
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
                    DocumentField documentField = fields.get(field);
                    if (documentField == null) {
                        documentField = new DocumentField(field, new ArrayList<>(2));
                        fields.put(field, documentField);
                    }
                    documentField.getValues().add(value);
                }
            }
        }

        BytesReference sourceFilteredAsBytes = sourceAsBytes;
        if (request.fetchSource() != null && request.fetchSource().fetchSource()) {
            sourceRequested = true;
            if (request.fetchSource().includes().length > 0 || request.fetchSource().excludes().length > 0) {
                Object value = sourceLookup.filter(request.fetchSource());
                try {
                    final int initialCapacity = Math.min(1024, sourceAsBytes.length());
                    BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
                    try (XContentBuilder builder = new XContentBuilder(sourceContentType.xContent(), streamOutput)) {
                        builder.value(value);
                        sourceFilteredAsBytes = builder.bytes();
                    }
                } catch (IOException e) {
                    throw new ElasticsearchException("Error filtering source", e);
                }
            }
        }

        // TODO when using delete/none, we can still return the source as bytes by generating it (using the sourceContentType)
        return new GetResult(concreteIndex, request.type(), request.id(), version, true,
                sourceRequested ? sourceFilteredAsBytes : null, fields);
    }

    public static class Result {

        private final Streamable action;
        private final DocWriteResponse.Result result;
        private final Map<String, Object> updatedSourceAsMap;
        private final XContentType updateSourceContentType;

        public Result(Streamable action, DocWriteResponse.Result result, Map<String, Object> updatedSourceAsMap,
                      XContentType updateSourceContentType) {
            this.action = action;
            this.result = result;
            this.updatedSourceAsMap = updatedSourceAsMap;
            this.updateSourceContentType = updateSourceContentType;
        }

        @SuppressWarnings("unchecked")
        public <T extends Streamable> T action() {
            return (T) action;
        }

        public DocWriteResponse.Result getResponseResult() {
            return result;
        }

        public Map<String, Object> updatedSourceAsMap() {
            return updatedSourceAsMap;
        }

        public XContentType updateSourceContentType() {
            return updateSourceContentType;
        }
    }

    /**
     * After executing the script, this is the type of operation that will be used for subsequent actions. This corresponds to the "ctx.op"
     * variable inside of scripts.
     */
    enum UpdateOpType {
        CREATE("create"),
        INDEX("index"),
        DELETE("delete"),
        NONE("none");

        private final String name;

        UpdateOpType(String name) {
            this.name = name;
        }

        public static UpdateOpType lenientFromString(String operation, Logger logger, String scriptId) {
            switch (operation) {
                case "create":
                    return UpdateOpType.CREATE;
                case "index":
                    return UpdateOpType.INDEX;
                case "delete":
                    return UpdateOpType.DELETE;
                case "none":
                    return UpdateOpType.NONE;
                default:
                    // TODO: can we remove this leniency yet??
                    logger.warn("Used upsert operation [{}] for script [{}], doing nothing...", operation, scriptId);
                    return UpdateOpType.NONE;
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Field names used to populate the script context
     */
    public static class ContextFields {
        public static final String CTX = "ctx";
        public static final String OP = "op";
        public static final String SOURCE = "_source";
        public static final String NOW = "_now";
        public static final String INDEX = "_index";
        public static final String TYPE = "_type";
        public static final String ID = "_id";
        public static final String VERSION = "_version";
        public static final String ROUTING = "_routing";
        public static final String PARENT = "_parent";
    }
}
