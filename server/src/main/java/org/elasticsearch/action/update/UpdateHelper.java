/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.DocValuesUpdateRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.UpdateNotSupportedException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateCtxMap;
import org.elasticsearch.script.UpdateScript;
import org.elasticsearch.script.UpsertCtxMap;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    public Result prepare(
        UpdateRequest request,
        IndexShard indexShard,
        LongSupplier nowInMillis,
        FetchSourceContext fetchSourceContext,
        SplitShardCountSummary splitShardCountSummary
    ) throws IOException {
        if (indexShard.indexSettings().sequenceNumbersDisabled()) {
            throw new UpdateNotSupportedException(indexShard.shardId());
        }
        // In-place doc-values fast path: when the partial document only sets doc_values.updatable scalar fields, the update needs
        // nothing from the current _source and can skip the read-modify-write, whose synthetic-source reconstruction dominates an
        // update on a columnar index. Only taken when no _source is echoed back and the index has no inference fields (both need the
        // merged source), and the document exists; a missing document falls through to the upsert path.
        final MappingLookup mappingLookup = indexShard.mapperService().mappingLookup();
        if (sourceReturnRequested(request) == false && mappingLookup.inferenceFields().isEmpty()) {
            List<Translog.DocValuesUpdate.FieldUpdate> updates = buildDocValuesFieldUpdates(request, mappingLookup);
            if (updates != null) {
                final GetResult getResult = indexShard.getService()
                    .getForUpdate(
                        request.id(),
                        request.routing(),
                        request.ifSeqNo(),
                        request.ifPrimaryTerm(),
                        FetchSourceContext.DO_NOT_FETCH_SOURCE,
                        splitShardCountSummary
                    );
                if (getResult.isExists()) {
                    String routing = calculateRouting(getResult, request.doc(), request.routing());
                    return new Result(
                        buildDocValuesUpdateRequest(request, getResult, routing, updates),
                        DocWriteResponse.Result.UPDATED,
                        null,
                        null
                    );
                }
            }
        }
        final GetResult getResult = indexShard.getService()
            .getForUpdate(
                request.id(),
                request.routing(),
                request.ifSeqNo(),
                request.ifPrimaryTerm(),
                fetchSourceContext,
                splitShardCountSummary
            );
        return prepare(indexShard, request, getResult, nowInMillis);
    }

    private static boolean sourceReturnRequested(UpdateRequest request) {
        return request.fetchSource() != null && request.fetchSource().fetchSource();
    }

    /**
     * First phase of a two-phase update preparation: resolves the updated document ahead of execution; {@code null}
     * when there is nothing worth holding until execution. OCC conditions are validated on consumption.
     */
    @Nullable
    public PreResolvedUpdate preResolve(
        UpdateRequest request,
        IndexShard indexShard,
        LongSupplier nowInMillis,
        FetchSourceContext fetchSourceContext,
        SplitShardCountSummary splitShardCountSummary
    ) {
        // The in-place doc-values fast path reads no _source, so skip pre-resolution (which prefetches stored fields) and let
        // prepare() take that path directly.
        final MappingLookup mappingLookup = indexShard.mapperService().mappingLookup();
        if (sourceReturnRequested(request) == false
            && mappingLookup.inferenceFields().isEmpty()
            && buildDocValuesFieldUpdates(request, mappingLookup) != null) {
            return null;
        }
        final Engine.GetResult getResult = indexShard.getService()
            .preResolveForUpdate(request.id(), request.routing(), splitShardCountSummary);
        // a missing document has nothing to prefetch (the upsert path keeps today's semantics), and holding a
        // translog-served get result would pin an in-memory copy of the document for the whole bulk while its reads
        // never touch stored fields
        if (getResult.exists() == false || getResult.isFromTranslog()) {
            getResult.close();
            return null;
        }
        return new PreResolvedUpdate(request, indexShard, nowInMillis, fetchSourceContext, getResult, splitShardCountSummary);
    }

    /**
     * An update preparation whose document was pre-resolved ahead of execution. {@link #complete()} consumes the
     * pre-resolved get and may be called at most once; closing releases the acquired searcher if the get was never
     * consumed.
     */
    public final class PreResolvedUpdate implements Releasable, ShardGetService.PreResolved {
        private final IndexShard indexShard;
        private final LongSupplier nowInMillis;
        private final FetchSourceContext fetchSourceContext;
        private final SplitShardCountSummary splitShardCountSummary;

        private UpdateRequest request;
        private Engine.GetResult preResolvedGet;

        private PreResolvedUpdate(
            UpdateRequest request,
            IndexShard indexShard,
            LongSupplier nowInMillis,
            FetchSourceContext fetchSourceContext,
            Engine.GetResult preResolvedGet,
            SplitShardCountSummary splitShardCountSummary
        ) {
            this.splitShardCountSummary = splitShardCountSummary;
            assert preResolvedGet != null;
            this.request = request;
            this.indexShard = indexShard;
            this.nowInMillis = nowInMillis;
            this.fetchSourceContext = fetchSourceContext;
            this.preResolvedGet = preResolvedGet;
        }

        /** Completes the preparation into an index or delete request or an update response. */
        public Result complete() throws IOException {
            if (isReleased()) {
                throw new IllegalStateException("pre-resolved update already consumed or closed");
            }
            final GetResult getResult = indexShard.getService()
                .getForUpdate(this, request.ifSeqNo(), request.ifPrimaryTerm(), fetchSourceContext, splitShardCountSummary);
            assert isReleased() : "expected the pre-resolved get to be consumed";
            return prepare(indexShard, request, getResult, nowInMillis);
        }

        public void prefetch(Map<LeafReader, StoredFields> storedFieldsCache) throws IOException {
            final var dav = preResolvedGet.docIdAndVersion();
            // Reuse the StoredFields instance per leaf reader: instantiation is cheap but Lucene's
            // CompressingStoredFieldsReader caches per-chunk decompression state inside the instance,
            // so sharing it across docs in the same segment avoids redundant work.
            StoredFields sf = storedFieldsCache.get(dav.reader);
            if (sf == null) {
                sf = dav.reader.storedFields();
                storedFieldsCache.put(dav.reader, sf);
            }
            sf.prefetch(dav.docId);
        }

        @Override
        public String id() {
            return request.id();
        }

        @Override
        @Nullable
        public String routing() {
            return request.routing();
        }

        @Override
        public Engine.GetResult takeGetResult() {
            final Engine.GetResult engineGetResult = preResolvedGet;
            assert engineGetResult != null : "pre-resolved get already consumed";
            preResolvedGet = null;
            return engineGetResult;
        }

        /** Whether the pre-resolved get has been consumed or released. */
        public boolean isReleased() {
            return preResolvedGet == null;
        }

        @Override
        public void close() {
            final Engine.GetResult engineGetResult = preResolvedGet;
            preResolvedGet = null;
            request = null;
            Releasables.close(engineGetResult);
        }
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action, in the event of a
     * noop).
     */
    protected Result prepare(IndexShard indexShard, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
        final boolean routingFromSlice = request.isRoutingFromSlice()
            || (indexShard.indexSettings() != null && indexShard.indexSettings().isSliceEnabled() && request.routing() != null);
        if (getResult.isExists() == false) {
            // If the document didn't exist, execute the update request as an upsert
            return prepareUpsert(indexShard.shardId(), request, getResult, nowInMillis, routingFromSlice);
        } else if (getResult.internalSourceRef() == null) {
            // no source, we can't do anything, throw a failure...
            throw new DocumentSourceMissingException(indexShard.shardId(), request.id());
        } else if (request.script() == null && request.doc() != null) {
            // The request has no script, it is a new doc that should be merged with the old document
            return prepareUpdateIndexRequest(indexShard, request, getResult, request.detectNoop(), routingFromSlice);
        } else {
            // The request has a script (or empty script), execute the script and prepare a new index request
            return prepareUpdateScriptRequest(indexShard, request, getResult, nowInMillis, routingFromSlice);
        }
    }

    /**
     * Execute a scripted upsert, where there is an existing upsert document and a script to be executed. The script is executed and a new
     * Tuple of operation and updated {@code _source} is returned.
     */
    Tuple<UpdateOpType, Map<String, Object>> executeScriptedUpsert(Script script, UpsertCtxMap ctxMap) {
        ctxMap = executeScript(script, ctxMap);
        UpdateOpType operation = UpdateOpType.lenientFromString(ctxMap.getMetadata().getOp(), logger, script.getIdOrCode());
        if (operation != UpdateOpType.CREATE && operation != UpdateOpType.NONE) {
            // Only valid options for an upsert script are "create" (the default) or "none", meaning abort upsert
            logger.warn("Invalid upsert operation [{}] for script [{}], doing nothing...", operation, script.getIdOrCode());
            operation = UpdateOpType.NONE;
        }

        return new Tuple<>(operation, ctxMap.getSource());
    }

    /**
     * Prepare the request for upsert, executing the upsert script if present, and returning a {@code Result} containing a new
     * {@code IndexRequest} to be executed on the primary and replicas.
     */
    Result prepareUpsert(
        ShardId shardId,
        UpdateRequest request,
        final GetResult getResult,
        LongSupplier nowInMillis,
        boolean routingFromSlice
    ) {
        if (request.upsertRequest() == null && request.docAsUpsert() == false) {
            throw new DocumentMissingException(shardId, request.id());
        }
        IndexRequest indexRequest = request.docAsUpsert() ? request.doc() : request.upsertRequest();
        if (request.scriptedUpsert() && request.script() != null) {
            // Run the script to perform the create logic
            IndexRequest upsert = request.upsertRequest();
            UpsertCtxMap ctxMap = new UpsertCtxMap(
                getResult.getIndex(),
                getResult.getId(),
                UpdateOpType.CREATE.toString(),
                nowInMillis.getAsLong(),
                upsert.sourceAsMap()
            );
            Tuple<UpdateOpType, Map<String, Object>> upsertResult = executeScriptedUpsert(request.script, ctxMap);
            switch (upsertResult.v1()) {
                case CREATE -> {
                    String index = request.index();
                    indexRequest = new IndexRequest(index).source(upsertResult.v2());
                }
                case NONE -> {
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
            .setRoutingFromSlice(routingFromSlice)
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
     * Calculate a routing value to be used, either the included index request's routing, retrieved document's routing when defined, or
     * in case the routing is stored as doc values, then the provided request routing is used as the routing.
     */
    @Nullable
    static String calculateRouting(GetResult getResult, @Nullable IndexRequest updateIndexRequest, @Nullable String requestRouting) {
        if (updateIndexRequest != null && updateIndexRequest.routing() != null) {
            return updateIndexRequest.routing();
        } else if (getResult.getFields().containsKey(SliceIndexing.FIELD_NAME)) {
            // A slice-enabled index surfaces the routing value as _slice rather than _routing.
            return getResult.field(SliceIndexing.FIELD_NAME).getValue().toString();
        } else if (getResult.getFields().containsKey(RoutingFieldMapper.NAME)) {
            return getResult.field(RoutingFieldMapper.NAME).getValue().toString();
        } else {
            return requestRouting;
        }
    }

    /**
     * Prepare the request for merging the existing document with a new one, can optionally detect a noop change. Returns a {@code Result}
     * containing a new {@code IndexRequest} to be executed on the primary and replicas.
     */
    Result prepareUpdateIndexRequest(
        IndexShard indexShard,
        UpdateRequest request,
        GetResult getResult,
        boolean detectNoop,
        boolean routingFromSlice
    ) {
        final IndexRequest currentRequest = request.doc();
        final String routing = calculateRouting(getResult, currentRequest, request.routing());
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> updatedSourceAsMap = sourceAndContent.v2();

        final boolean noop = XContentHelper.update(updatedSourceAsMap, currentRequest.sourceAsMap(), detectNoop) == false;

        // We can only actually turn the update into a noop if detectNoop is true to preserve backwards compatibility and to handle cases
        // where users repopulating multi-fields or adding synonyms, etc.
        if (detectNoop && noop) {
            UpdateResponse update = new UpdateResponse(
                indexShard.shardId(),
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
                    indexShard.mapperService().mappingLookup(),
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
            String index = request.index();
            DocValuesUpdateRequest docValuesUpdate = tryBuildDocValuesUpdate(indexShard, request, getResult, routing);
            if (docValuesUpdate != null) {
                return new Result(docValuesUpdate, DocWriteResponse.Result.UPDATED, updatedSourceAsMap, updateSourceContentType);
            }
            IndexRequest finalIndexRequest = new IndexRequest(index).id(request.id())
                .routing(routing)
                .setRoutingFromSlice(routingFromSlice)
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
     * If the partial document of this update touches only {@code doc_values.updatable} fields with scalar values, build the in-place
     * doc-values update that replaces the read-modify-reindex. Returns {@code null} — falling back to a normal index update — whenever
     * anything makes the fast path inapplicable (feature flag off, no updatable fields, a non-updatable or non-scalar field, a null
     * value that would remove a value, etc.).
     */
    private static DocValuesUpdateRequest tryBuildDocValuesUpdate(
        IndexShard indexShard,
        UpdateRequest request,
        GetResult getResult,
        String routing
    ) {
        List<Translog.DocValuesUpdate.FieldUpdate> updates = buildDocValuesFieldUpdates(
            request,
            indexShard.mapperService().mappingLookup()
        );
        if (updates == null) {
            return null;
        }
        return buildDocValuesUpdateRequest(request, getResult, routing, updates);
    }

    /**
     * Encodes the partial document into in-place doc-values field updates, or {@code null} when the fast path does not apply. Derived
     * purely from the request and mapping, so it can gate the fast path before the document is fetched.
     */
    private static List<Translog.DocValuesUpdate.FieldUpdate> buildDocValuesFieldUpdates(
        UpdateRequest request,
        MappingLookup mappingLookup
    ) {
        if (FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled() == false) {
            return null;
        }
        Set<String> updatableFields = mappingLookup.updatableFields();
        if (updatableFields.isEmpty()) {
            return null;
        }
        // A scripted update or a bare upsert has no partial document to apply in place.
        if (request.script() != null || request.doc() == null) {
            return null;
        }
        // A user-supplied optimistic-concurrency precondition cannot be honored on the in-place path: a doc-values update does not change
        // the document's seq_no, so it is invisible to seq_no-based CAS. Fall back to the read-modify-reindex update, which enforces it.
        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO || request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            return null;
        }
        Map<String, Object> partialDoc = request.doc().sourceAsMap();
        if (partialDoc.isEmpty()) {
            return null;
        }
        List<Translog.DocValuesUpdate.FieldUpdate> updates = new ArrayList<>(partialDoc.size());
        FieldMapper.DocValuesUpdateSink sink = new FieldMapper.DocValuesUpdateSink() {
            @Override
            public void numeric(String field, long value) {
                updates.add(new Translog.DocValuesUpdate.NumericFieldUpdate(field, value));
            }

            @Override
            public void binary(String field, BytesRef value) {
                updates.add(new Translog.DocValuesUpdate.BinaryFieldUpdate(field, value));
            }
        };
        for (Map.Entry<String, Object> entry : partialDoc.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            if (updatableFields.contains(field) == false) {
                return null;
            }
            // Only scalar replacements can be applied in place: a null removes the value, a map/list is a nested or multi-valued update.
            if (value == null || value instanceof Map || value instanceof Iterable) {
                return null;
            }
            if (mappingLookup.getMapper(field) instanceof FieldMapper fieldMapper && fieldMapper.isDocValuesUpdatable()) {
                fieldMapper.encodeDocValuesUpdate(value, sink);
            } else {
                return null;
            }
        }
        return updates;
    }

    private static DocValuesUpdateRequest buildDocValuesUpdateRequest(
        UpdateRequest request,
        GetResult getResult,
        String routing,
        List<Translog.DocValuesUpdate.FieldUpdate> updates
    ) {
        DocValuesUpdateRequest docValuesUpdateRequest = new DocValuesUpdateRequest(request.index(), request.id(), updates).routing(routing)
            .documentVersion(getResult.getVersion())
            .setIfSeqNo(getResult.getSeqNo())
            .setIfPrimaryTerm(getResult.getPrimaryTerm());
        // Carry over the write parameters, exactly as the reindex path does for its IndexRequest, so e.g. the refresh policy of the
        // single-document update API is honoured.
        docValuesUpdateRequest.waitForActiveShards(request.waitForActiveShards()).timeout(request.timeout());
        docValuesUpdateRequest.setRefreshPolicy(request.getRefreshPolicy());
        return docValuesUpdateRequest;
    }

    /**
     * Prepare the request for updating an existing document using a script. Executes the script and returns a {@code Result} containing
     * either a new {@code IndexRequest} or {@code DeleteRequest} (depending on the script's returned "op" value) to be executed on the
     * primary and replicas.
     */
    Result prepareUpdateScriptRequest(
        IndexShard indexShard,
        UpdateRequest request,
        GetResult getResult,
        LongSupplier nowInMillis,
        boolean routingFromSlice
    ) {
        final IndexRequest currentRequest = request.doc();
        final String routing = calculateRouting(getResult, currentRequest, request.routing());
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();

        UpdateCtxMap ctxMap = executeScript(
            request.script,
            new UpdateCtxMap(
                getResult.getIndex(),
                getResult.getId(),
                getResult.getVersion(),
                routing,
                MapperService.SINGLE_MAPPING_NAME,
                UpdateOpType.INDEX.toString(), // The default operation is "index"
                nowInMillis.getAsLong(),
                sourceAndContent.v2()
            )
        );
        UpdateOpType operation = UpdateOpType.lenientFromString(ctxMap.getMetadata().getOp(), logger, request.script.getIdOrCode());
        final Map<String, Object> updatedSourceAsMap = ctxMap.getSource();

        switch (operation) {
            case INDEX -> {
                String index = request.index();
                IndexRequest indexRequest = new IndexRequest(index).id(request.id())
                    .routing(routing)
                    .setRoutingFromSlice(routingFromSlice)
                    .source(updatedSourceAsMap, updateSourceContentType)
                    .setIfSeqNo(getResult.getSeqNo())
                    .setIfPrimaryTerm(getResult.getPrimaryTerm())
                    .waitForActiveShards(request.waitForActiveShards())
                    .timeout(request.timeout())
                    .setRefreshPolicy(request.getRefreshPolicy());
                return new Result(indexRequest, DocWriteResponse.Result.UPDATED, updatedSourceAsMap, updateSourceContentType);
            }
            case DELETE -> {
                String index = request.index();
                DeleteRequest deleteRequest = new DeleteRequest(index).id(request.id())
                    .routing(routing)
                    .setRoutingFromSlice(routingFromSlice)
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
                    indexShard.shardId(),
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
                        indexShard.mapperService().mappingLookup(),
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

    private <T extends UpdateCtxMap> T executeScript(Script script, T ctxMap) {
        try {
            if (scriptService != null) {
                UpdateScript.Factory factory = scriptService.compile(script, UpdateScript.CONTEXT);
                UpdateScript executableScript = factory.newInstance(script.getParams(), ctxMap);
                executableScript.execute();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to execute script", e);
        }
        return ctxMap;
    }

    /**
     * Applies {@link UpdateRequest#fetchSource()} to the _source of the updated document to be returned in a update response.
     * // TODO can we pass a Source here rather than Map, XcontentType and BytesReference?
     */
    public static GetResult extractGetResult(
        final UpdateRequest request,
        String concreteIndex,
        final MappingLookup mappingLookup,
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
        SourceFilter sourceFilter = request.fetchSource().filter();
        if (sourceFilter != null) {
            sourceFilteredAsBytes = Source.fromMap(source, sourceContentType).filter(sourceFilter).internalSourceRef();
        } else if (sourceFilteredAsBytes == null && source != null) {
            // Rebuild the source bytes from the merged map when the caller has none, e.g. an in-place doc-values update carries no
            // full-document source of its own.
            sourceFilteredAsBytes = Source.fromMap(source, sourceContentType).internalSourceRef();
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
                case "noop":
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
    }
}
