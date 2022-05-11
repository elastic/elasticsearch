/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.update;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateScript;
import org.elasticsearch.script.field.Metadata;
import org.elasticsearch.script.field.Op;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Helper for translating an update request to an index, delete request or update response.
 */
public class UpdateHelper {

    private static final Logger logger = LogManager.getLogger(UpdateHelper.class);

    private final ScriptService scriptService;

    public UpdateHelper(ScriptService scriptService) {
        this.scriptService = scriptService;
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action).
     */
    public Result prepare(UpdateRequest request, IndexShard indexShard, LongSupplier nowInMillis) throws IOException {
        final GetResult getResult = indexShard.getService().getForUpdate(request.id(), request.ifSeqNo(), request.ifPrimaryTerm());
        return prepare(indexShard.shardId(), request, getResult, nowInMillis);
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action, in the event of a
     * noop).
     */
    protected Result prepare(ShardId shardId, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
        if (getResult.isExists() == false) {
            // If the document didn't exist, execute the update request as an upsert
            return prepareUpsert(shardId, request, getResult, nowInMillis);
        } else if (getResult.internalSourceRef() == null) {
            // no source, we can't do anything, throw a failure...
            throw new DocumentSourceMissingException(shardId, request.id());
        } else if (request.script() == null && request.doc() != null) {
            // The request has no script, it is a new doc that should be merged with the old document
            return prepareUpdateIndexRequest(shardId, request, getResult, request.detectNoop());
        } else {
            // The request has a script (or empty script), execute the script and prepare a new index request
            return prepareUpdateScriptRequest(shardId, request, getResult, nowInMillis); // _update with upsert, _update
        }
    }

    /**
     * Execute a scripted upsert, where there is an existing upsert document and a script to be executed. The script is executed and a new
     * Tuple of operation and updated {@code _source} is returned.
     */
    Tuple<Op, Map<String, Object>> executeScriptedUpsert(Map<String, Object> upsertDoc, Script script, LongSupplier nowInMillis) {
        Map<String, Object> ctx = Maps.newMapWithExpectedSize(3); // source, op,
        ctx.put(ContextFields.SOURCE, upsertDoc);
        // Tell the script that this is a create and not an update
        UpsertMetadata metadata = new UpsertMetadata(ctx, Op.CREATE, nowInMillis.getAsLong());
        ctx = executeScript(script, metadata, ctx);

        Op operation = getOp(metadata, script.getIdOrCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> newSource = (Map<String, Object>) ctx.get(ContextFields.SOURCE);

        // TODO(stu): should we disallow setting operation to null?
        if (operation != Op.CREATE && operation != Op.NOOP) {
            // Only valid options for an upsert script are "create" (the default) or "none", meaning abort upsert
            logger.warn("Invalid upsert operation [{}] for script [{}], doing nothing...", operation, script.getIdOrCode());
            operation = Op.NOOP;
        }

        return new Tuple<>(operation, newSource);
    }

    // Get the operation from metadata, if an unknown operation, throw an IllegalArgumentException
    // Previously, unknown operations would be changed to noop.
    protected Op getOp(Metadata md, String scriptId) {
        Op op = md.getOp();
        if (op != Op.UNKOWN) {
            return op;
        }
        throw new IllegalArgumentException(
            "Operation type ["
                + md.rawOp()
                + "] not allowed for script ["
                + scriptId
                + "], only "
                + String.join(",", md.validOps())
                + " are allowed"
        );
    }

    /**
     * Prepare the request for upsert, executing the upsert script if present, and returning a {@code Result} containing a new
     * {@code IndexRequest} to be executed on the primary and replicas.
     */
    Result prepareUpsert(ShardId shardId, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
        if (request.upsertRequest() == null && request.docAsUpsert() == false) {
            throw new DocumentMissingException(shardId, request.id());
        }
        IndexRequest indexRequest = request.docAsUpsert() ? request.doc() : request.upsertRequest();
        if (request.scriptedUpsert() && request.script() != null) {
            // Run the script to perform the create logic
            IndexRequest upsert = request.upsertRequest();
            Tuple<Op, Map<String, Object>> upsertResult = executeScriptedUpsert(upsert.sourceAsMap(), request.script, nowInMillis);
            switch (upsertResult.v1()) {
                case CREATE -> indexRequest = Requests.indexRequest(request.index()).source(upsertResult.v2());
                case NOOP -> {
                    UpdateResponse update = new UpdateResponse(
                        shardId,
                        getResult.getId(),
                        getResult.getSeqNo(),
                        getResult.getPrimaryTerm(),
                        getResult.getVersion(),
                        DocWriteResponse.Result.NOOP
                    );
                    update.setGetResult(getResult);
                    return new Result(update, DocWriteResponse.Result.NOOP, upsertResult.v2(), XContentType.JSON);
                }
                default ->
                    // It's fine to throw an exception here, the leniency is handled/logged by `executeScriptedUpsert`
                    throw new IllegalArgumentException("unknown upsert operation, got: " + upsertResult.v1());
            }
        }

        indexRequest.index(request.index())
            .id(request.id())
            .setRefreshPolicy(request.getRefreshPolicy())
            .routing(request.routing())
            .timeout(request.timeout())
            .waitForActiveShards(request.waitForActiveShards())
            // it has to be a "create!"
            .create(true);

        if (request.versionType() != VersionType.INTERNAL) {
            // in all but the internal versioning mode, we want to create the new document using the given version.
            indexRequest.version(request.version()).versionType(request.versionType());
        }

        return new Result(indexRequest, DocWriteResponse.Result.CREATED, null, null);
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
     * Prepare the request for merging the existing document with a new one, can optionally detect a noop change. Returns a {@code Result}
     * containing a new {@code IndexRequest} to be executed on the primary and replicas.
     */
    static Result prepareUpdateIndexRequest(ShardId shardId, UpdateRequest request, GetResult getResult, boolean detectNoop) {
        final IndexRequest currentRequest = request.doc();
        final String routing = calculateRouting(getResult, currentRequest);
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> updatedSourceAsMap = sourceAndContent.v2();

        final boolean noop = XContentHelper.update(updatedSourceAsMap, currentRequest.sourceAsMap(), detectNoop) == false;

        // We can only actually turn the update into a noop if detectNoop is true to preserve backwards compatibility and to handle cases
        // where users repopulating multi-fields or adding synonyms, etc.
        if (detectNoop && noop) {
            UpdateResponse update = new UpdateResponse(
                shardId,
                getResult.getId(),
                getResult.getSeqNo(),
                getResult.getPrimaryTerm(),
                getResult.getVersion(),
                DocWriteResponse.Result.NOOP
            );
            update.setGetResult(
                extractGetResult(
                    request,
                    request.index(),
                    getResult.getSeqNo(),
                    getResult.getPrimaryTerm(),
                    getResult.getVersion(),
                    updatedSourceAsMap,
                    updateSourceContentType,
                    getResult.internalSourceRef()
                )
            );
            return new Result(update, DocWriteResponse.Result.NOOP, updatedSourceAsMap, updateSourceContentType);
        } else {
            final IndexRequest finalIndexRequest = Requests.indexRequest(request.index())
                .id(request.id())
                .routing(routing)
                .source(updatedSourceAsMap, updateSourceContentType)
                .setIfSeqNo(getResult.getSeqNo())
                .setIfPrimaryTerm(getResult.getPrimaryTerm())
                .waitForActiveShards(request.waitForActiveShards())
                .timeout(request.timeout())
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
        final IndexRequest currentRequest = request.doc();
        final String routing = calculateRouting(getResult, currentRequest);
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> sourceAsMap = sourceAndContent.v2();

        // TODO(stu): metadata _update with script, upsert
        Map<String, Object> ctx = Maps.newMapWithExpectedSize(16);
        ctx.put(ContextFields.TYPE, MapperService.SINGLE_MAPPING_NAME);
        ctx.put(ContextFields.SOURCE, sourceAsMap);
        // The default operation is "index"
        UpdateMetadata metadata = new UpdateMetadata(
            ctx,
            getResult.getIndex(),
            getResult.getId(),
            routing,
            getResult.getVersion(),
            Op.INDEX,
            nowInMillis.getAsLong()
        );
        ctx = executeScript(request.script, metadata, ctx);

        Op operation = getOp(metadata, request.script.getIdOrCode());

        @SuppressWarnings("unchecked")
        final Map<String, Object> updatedSourceAsMap = (Map<String, Object>) ctx.get(ContextFields.SOURCE);

        switch (operation) {
            case INDEX -> {
                final IndexRequest indexRequest = Requests.indexRequest(request.index())
                    .id(request.id())
                    .routing(routing)
                    .source(updatedSourceAsMap, updateSourceContentType)
                    .setIfSeqNo(getResult.getSeqNo())
                    .setIfPrimaryTerm(getResult.getPrimaryTerm())
                    .waitForActiveShards(request.waitForActiveShards())
                    .timeout(request.timeout())
                    .setRefreshPolicy(request.getRefreshPolicy());
                return new Result(indexRequest, DocWriteResponse.Result.UPDATED, updatedSourceAsMap, updateSourceContentType);
            }
            case DELETE -> {
                DeleteRequest deleteRequest = Requests.deleteRequest(request.index())
                    .id(request.id())
                    .routing(routing)
                    .setIfSeqNo(getResult.getSeqNo())
                    .setIfPrimaryTerm(getResult.getPrimaryTerm())
                    .waitForActiveShards(request.waitForActiveShards())
                    .timeout(request.timeout())
                    .setRefreshPolicy(request.getRefreshPolicy());
                return new Result(deleteRequest, DocWriteResponse.Result.DELETED, updatedSourceAsMap, updateSourceContentType);
            }
            default -> {
                // If it was neither an INDEX or DELETE operation, treat it as a noop
                UpdateResponse update = new UpdateResponse(
                    shardId,
                    getResult.getId(),
                    getResult.getSeqNo(),
                    getResult.getPrimaryTerm(),
                    getResult.getVersion(),
                    DocWriteResponse.Result.NOOP
                );
                update.setGetResult(
                    extractGetResult(
                        request,
                        request.index(),
                        getResult.getSeqNo(),
                        getResult.getPrimaryTerm(),
                        getResult.getVersion(),
                        updatedSourceAsMap,
                        updateSourceContentType,
                        getResult.internalSourceRef()
                    )
                );
                return new Result(update, DocWriteResponse.Result.NOOP, updatedSourceAsMap, updateSourceContentType);
            }
        }
    }

    private Map<String, Object> executeScript(Script script, Metadata metadata, Map<String, Object> ctx) {
        try {
            if (scriptService != null) {
                UpdateScript.Factory factory = scriptService.compile(script, UpdateScript.CONTEXT);
                UpdateScript executableScript = factory.newInstance(script.getParams(), metadata, ctx);
                executableScript.execute();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to execute script", e);
        }
        return ctx;
    }

    /**
     * Applies {@link UpdateRequest#fetchSource()} to the _source of the updated document to be returned in a update response.
     */
    public static GetResult extractGetResult(
        final UpdateRequest request,
        String concreteIndex,
        long seqNo,
        long primaryTerm,
        long version,
        final Map<String, Object> source,
        XContentType sourceContentType,
        @Nullable final BytesReference sourceAsBytes
    ) {
        if (request.fetchSource() == null || request.fetchSource().fetchSource() == false) {
            return null;
        }

        BytesReference sourceFilteredAsBytes = sourceAsBytes;
        if (request.fetchSource().includes().length > 0 || request.fetchSource().excludes().length > 0) {
            SourceLookup sourceLookup = new SourceLookup();
            sourceLookup.setSource(source);
            Object value = sourceLookup.filter(request.fetchSource());
            try {
                final int initialCapacity = sourceAsBytes != null ? Math.min(1024, sourceAsBytes.length()) : 1024;
                BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
                try (XContentBuilder builder = new XContentBuilder(sourceContentType.xContent(), streamOutput)) {
                    builder.value(value);
                    sourceFilteredAsBytes = BytesReference.bytes(builder);
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Error filtering source", e);
            }
        }

        // TODO when using delete/none, we can still return the source as bytes by generating it (using the sourceContentType)
        return new GetResult(
            concreteIndex,
            request.id(),
            seqNo,
            primaryTerm,
            version,
            true,
            sourceFilteredAsBytes,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
    }

    public static class Result {

        private final Writeable action;
        private final DocWriteResponse.Result result;
        private final Map<String, Object> updatedSourceAsMap;
        private final XContentType updateSourceContentType;

        public Result(
            Writeable action,
            DocWriteResponse.Result result,
            Map<String, Object> updatedSourceAsMap,
            XContentType updateSourceContentType
        ) {
            this.action = action;
            this.result = result;
            this.updatedSourceAsMap = updatedSourceAsMap;
            this.updateSourceContentType = updateSourceContentType;
        }

        @SuppressWarnings("unchecked")
        public <T extends Writeable> T action() {
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
     * Field names used to populate the script context
     */
    // TODO(stu): do we need this other than SOURCE?
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
    }

    /**
     * The old way of spelling Op.NOOP, Metadata will read this String but does not write it.
     */
    private static final String LEGACY_NOOP_STRING = "none";

    private static Op opFromString(String opStr) {
        if (opStr == null) {
            return null;
        } else if (LEGACY_NOOP_STRING.equals(opStr.toLowerCase(Locale.ROOT))) {
            return Op.NOOP;
        } else {
            return Op.fromString(opStr);
        }
    }

    /**
     * Metadata for Updates via {@link UpdateScript} with script and upsert.
     *
     * _index, _id, _routing, _version and _now (timestamp) are read-only.
     *
     * _op is read/write with valid values: NOOP ("none"), INDEX, DELETE, CREATE
     *
     * _version_type is unavailable.
     */
    private static class UpdateMetadata extends org.elasticsearch.script.field.Metadata {
        private final ZonedDateTime timestamp;

        UpdateMetadata(Map<String, Object> ctx, String index, String id, String routing, Long version, Op op, long timestamp) {
            super(
                ctx,
                ContextFields.INDEX,
                ContextFields.ID,
                ContextFields.ROUTING,
                ContextFields.VERSION,
                null,
                ContextFields.OP,
                EnumSet.of(Op.NOOP, Op.INDEX, Op.DELETE, Op.CREATE)
            );
            this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
            put(ContextFields.NOW, timestamp);
            put(indexKey, index);
            put(idKey, id);
            put(routingKey, routing);
            put(versionKey, version);
            setOp(Objects.requireNonNull(op));
        }

        @Override
        public void setIndex(String index) {
            unsupported(INDEX, true);
        }

        @Override
        public void setId(String id) {
            unsupported(ID, true);
        }

        @Override
        public void setRouting(String routing) {
            unsupported(ROUTING, true);
        }

        @Override
        public void setVersion(Long version) {
            unsupported(VERSION, true);
        }

        @Override
        public Op getOp() {
            return opFromString(getString(opKey));
        }

        @Override
        public void setOp(Op op) {
            validateOp(op);
            put(opKey, op.toString());
        }

        @Override
        public ZonedDateTime getTimestamp() {
            return timestamp;
        }

        @Override
        public List<String> validOps() {
            List<String> enumOps = super.validOps();
            List<String> ops = new ArrayList<>(enumOps.size() + 1);
            ops.addAll(enumOps);
            ops.add("none");
            return ops;
        }
    }

    /**
     * Metadata for insertions done via scripted upsert with an {@link UpdateScript}
     *
     * The only metadata available is the timestamp and the Op.
     * The Op must be either "create" or "none" (Op.NOOP).
     *
     * _index, _id, _routing, _version, _version_type are unavailable.
     */
    private static class UpsertMetadata extends org.elasticsearch.script.field.Metadata {
        private final ZonedDateTime timestamp;

        UpsertMetadata(Map<String, Object> ctx, Op op, long timestamp) {
            super(ctx, null, null, null, null, null, ContextFields.OP, EnumSet.of(Op.CREATE, Op.NOOP));
            this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
            put(ContextFields.NOW, timestamp);
            setOp(op);
        }

        @Override
        public Op getOp() {
            return opFromString(getString(opKey));
        }

        @Override
        public void setOp(Op op) {
            validateOp(op);
            put(opKey, op.toString());
        }

        @Override
        public ZonedDateTime getTimestamp() {
            return timestamp;
        }

        @Override
        public List<String> validOps() {
            List<String> enumOps = super.validOps();
            List<String> ops = new ArrayList<>(enumOps.size() + 1);
            ops.addAll(enumOps);
            ops.add("none");
            return ops;
        }
    }
}
