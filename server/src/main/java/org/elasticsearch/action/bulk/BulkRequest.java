/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.transport.RawIndexingDataTransportRequest;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A bulk request holds an ordered {@link IndexRequest}s, {@link DeleteRequest}s and {@link UpdateRequest}s
 * and allows to execute it in a single batch.
 *
 * Note that we only support refresh on the bulk request not per item.
 * @see org.elasticsearch.client.internal.Client#bulk(BulkRequest)
 */
public class BulkRequest extends ActionRequest
    implements
        CompositeIndicesRequest,
        WriteRequest<BulkRequest>,
        Accountable,
        RawIndexingDataTransportRequest {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkRequest.class);

    private static final int REQUEST_OVERHEAD = 50;

    /**
     * Requests that are part of this request. It is only possible to add things that are both {@link ActionRequest}s and
     * {@link WriteRequest}s to this but java doesn't support syntax to declare that everything in the array has both types so we declare
     * the one with the least casts.
     */
    final List<DocWriteRequest<?>> requests = new ArrayList<>();
    private final Set<String> indices = new HashSet<>();

    protected TimeValue timeout = BulkShardRequest.DEFAULT_TIMEOUT;
    private IncrementalState incrementalState = IncrementalState.EMPTY;
    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;
    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;
    private String globalPipeline;
    private String globalRouting;
    private String globalIndex;
    private Boolean globalRequireAlias;
    private Boolean globalRequireDatsStream;
    private boolean includeSourceOnError = true;

    private long sizeInBytes = 0;

    public BulkRequest() {}

    public BulkRequest(StreamInput in) throws IOException {
        super(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        requests.addAll(in.readCollectionAsList(i -> DocWriteRequest.readDocumentRequest(null, i)));
        refreshPolicy = RefreshPolicy.readFrom(in);
        timeout = in.readTimeValue();
        for (DocWriteRequest<?> request : requests) {
            indices.add(Objects.requireNonNull(request.index(), "request index must not be null"));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            incrementalState = new BulkRequest.IncrementalState(in);
        } else {
            incrementalState = BulkRequest.IncrementalState.EMPTY;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.INGEST_REQUEST_INCLUDE_SOURCE_ON_ERROR)) {
            includeSourceOnError = in.readBoolean();
        } // else default value is true
    }

    public BulkRequest(@Nullable String globalIndex) {
        this.globalIndex = globalIndex;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public BulkRequest add(DocWriteRequest<?>... requests) {
        for (DocWriteRequest<?> request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Add a request to the current BulkRequest.
     *
     * Note for internal callers: This method does not respect all global parameters.
     *                            Only the global index is applied to the request objects.
     *                            Global parameters would be respected if the request was serialized for a REST call as it is
     *                            in the high level rest client.
     * @param request Request to add
     * @return the current bulk request
     */
    public BulkRequest add(DocWriteRequest<?> request) {
        if (request instanceof IndexRequest indexRequest) {
            add(indexRequest);
        } else if (request instanceof DeleteRequest deleteRequest) {
            add(deleteRequest);
        } else if (request instanceof UpdateRequest updateRequest) {
            add(updateRequest);
        } else {
            throw new IllegalArgumentException("No support for request [" + request + "]");
        }
        indices.add(request.index());
        return this;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public BulkRequest add(Iterable<DocWriteRequest<?>> requests) {
        for (DocWriteRequest<?> request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkRequest add(IndexRequest request) {
        return internalAdd(request);
    }

    BulkRequest internalAdd(IndexRequest request) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        // lack of source is validated in validate() method
        sizeInBytes += (request.source() != null ? request.source().length() : 0) + REQUEST_OVERHEAD;
        indices.add(request.index());
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public BulkRequest add(UpdateRequest request) {
        return internalAdd(request);
    }

    BulkRequest internalAdd(UpdateRequest request) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        if (request.doc() != null) {
            sizeInBytes += request.doc().source().length();
        }
        if (request.upsertRequest() != null) {
            sizeInBytes += request.upsertRequest().source().length();
        }
        if (request.script() != null) {
            sizeInBytes += request.script().getIdOrCode().length() * 2;
        }
        indices.add(request.index());
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkRequest add(DeleteRequest request) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        sizeInBytes += REQUEST_OVERHEAD;
        indices.add(request.index());
        return this;
    }

    /**
     * The list of requests in this bulk request.
     */
    public List<DocWriteRequest<?>> requests() {
        return this.requests;
    }

    /**
     * The number of actions in the bulk request.
     */
    public int numberOfActions() {
        return requests.size();
    }

    /**
     * The estimated size in bytes of the bulk request.
     */
    public long estimatedSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(byte[] data, int from, int length, XContentType xContentType) throws IOException {
        return add(data, from, length, null, xContentType);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(byte[] data, int from, int length, @Nullable String defaultIndex, XContentType xContentType) throws IOException {
        return add(new BytesArray(data, from, length), defaultIndex, xContentType);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, XContentType xContentType) throws IOException {
        return add(data, defaultIndex, null, null, null, null, null, null, true, xContentType, RestApiVersion.current());
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, boolean allowExplicitIndex, XContentType xContentType)
        throws IOException {
        return add(data, defaultIndex, null, null, null, null, null, null, allowExplicitIndex, xContentType, RestApiVersion.current());

    }

    public BulkRequest add(
        BytesReference data,
        @Nullable String defaultIndex,
        @Nullable String defaultRouting,
        @Nullable FetchSourceContext defaultFetchSourceContext,
        @Nullable String defaultPipeline,
        @Nullable Boolean defaultRequireAlias,
        @Nullable Boolean defaultRequireDataStream,
        @Nullable Boolean defaultListExecutedPipelines,
        boolean allowExplicitIndex,
        XContentType xContentType,
        RestApiVersion restApiVersion
    ) throws IOException {
        String routing = valueOrDefault(defaultRouting, globalRouting);
        String pipeline = valueOrDefault(defaultPipeline, globalPipeline);
        Boolean requireAlias = valueOrDefault(defaultRequireAlias, globalRequireAlias);
        Boolean requireDataStream = valueOrDefault(defaultRequireDataStream, globalRequireDatsStream);
        new BulkRequestParser(true, includeSourceOnError, restApiVersion).parse(
            data,
            defaultIndex,
            routing,
            defaultFetchSourceContext,
            pipeline,
            requireAlias,
            requireDataStream,
            defaultListExecutedPipelines,
            allowExplicitIndex,
            xContentType,
            (indexRequest, type) -> internalAdd(indexRequest),
            this::internalAdd,
            this::add
        );
        return this;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public BulkRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public BulkRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    @Override
    public BulkRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final BulkRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public void incrementalState(IncrementalState incrementalState) {
        this.incrementalState = incrementalState;
    }

    public final BulkRequest includeSourceOnError(boolean includeSourceOnError) {
        this.includeSourceOnError = includeSourceOnError;
        return this;
    }

    /**
     * Note for internal callers (NOT high level rest client),
     * the global parameter setting is ignored when used with:
     *
     * - {@link BulkRequest#add(IndexRequest)}
     * - {@link BulkRequest#add(UpdateRequest)}
     * - {@link BulkRequest#add(DocWriteRequest)}
     * - {@link BulkRequest#add(DocWriteRequest[])} )}
     * - {@link BulkRequest#add(Iterable)}
     * @param globalPipeline the global default setting
     * @return Bulk request with global setting set
     */
    public final BulkRequest pipeline(String globalPipeline) {
        this.globalPipeline = globalPipeline;
        return this;
    }

    /**
     * Note for internal callers (NOT high level rest client),
     * the global parameter setting is ignored when used with:
     *
      - {@link BulkRequest#add(IndexRequest)}
      - {@link BulkRequest#add(UpdateRequest)}
      - {@link BulkRequest#add(DocWriteRequest)}
      - {@link BulkRequest#add(DocWriteRequest[])} )}
      - {@link BulkRequest#add(Iterable)}
     * @param globalRouting the global default setting
     * @return Bulk request with global setting set
     */
    public final BulkRequest routing(String globalRouting) {
        this.globalRouting = globalRouting;
        return this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public IncrementalState incrementalState() {
        return incrementalState;
    }

    public String pipeline() {
        return globalPipeline;
    }

    public String routing() {
        return globalRouting;
    }

    public Boolean requireAlias() {
        return globalRequireAlias;
    }

    public Boolean requireDataStream() {
        return globalRequireDatsStream;
    }

    public boolean includeSourceOnError() {
        return includeSourceOnError;
    }

    /**
     * Note for internal callers (NOT high level rest client),
     * the global parameter setting is ignored when used with:
     *
     * - {@link BulkRequest#add(IndexRequest)}
     * - {@link BulkRequest#add(UpdateRequest)}
     * - {@link BulkRequest#add(DocWriteRequest)}
     * - {@link BulkRequest#add(DocWriteRequest[])} )}
     * - {@link BulkRequest#add(Iterable)}
     * @param globalRequireAlias the global default setting
     * @return Bulk request with global setting set
     */
    public BulkRequest requireAlias(Boolean globalRequireAlias) {
        this.globalRequireAlias = globalRequireAlias;
        return this;
    }

    public BulkRequest requireDataStream(Boolean globalRequireDatsStream) {
        this.globalRequireDatsStream = globalRequireDatsStream;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (DocWriteRequest<?> request : requests) {
            // We first check if refresh has been set
            if (((WriteRequest<?>) request).getRefreshPolicy() != RefreshPolicy.NONE) {
                validationException = addValidationError(
                    "RefreshPolicy is not supported on an item request. Set it on the BulkRequest instead.",
                    validationException
                );
            }
            ActionRequestValidationException ex = ((WriteRequest<?>) request).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeCollection(requests, DocWriteRequest::writeDocumentRequest);
        refreshPolicy.writeTo(out);
        out.writeTimeValue(timeout);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            incrementalState.writeTo(out);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.INGEST_REQUEST_INCLUDE_SOURCE_ON_ERROR)) {
            out.writeBoolean(includeSourceOnError);
        }
    }

    @Override
    public String getDescription() {
        return "requests[" + requests.size() + "], indices[" + Strings.collectionToDelimitedString(indices, ", ") + "]";
    }

    private void applyGlobalMandatoryParameters(DocWriteRequest<?> request) {
        request.index(valueOrDefault(request.index(), globalIndex));
    }

    private static String valueOrDefault(String value, String globalDefault) {
        if (Strings.isNullOrEmpty(value) && Strings.isNullOrEmpty(globalDefault) == false) {
            return globalDefault;
        }
        return value;
    }

    private static Boolean valueOrDefault(Boolean value, Boolean globalDefault) {
        if (Objects.isNull(value) && Objects.isNull(globalDefault) == false) {
            return globalDefault;
        }
        return value;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + requests.stream().mapToLong(Accountable::ramBytesUsed).sum();
    }

    public Set<String> getIndices() {
        return Collections.unmodifiableSet(indices);
    }

    /**
     * Returns true if this is a request for a simulation rather than a real bulk request.
     * @return true if this is a simulated bulk request
     */
    public boolean isSimulated() {
        return false; // Always false, but may be overridden by a subclass
    }

    /*
     * Returns any component template substitutions that are to be used as part of this bulk request. We would likely only have
     * substitutions in the event of a simulated request.
     */
    public Map<String, ComponentTemplate> getComponentTemplateSubstitutions() throws IOException {
        return Map.of();
    }

    public Map<String, ComposableIndexTemplate> getIndexTemplateSubstitutions() throws IOException {
        return Map.of();
    }

    record IncrementalState(Map<ShardId, Exception> shardLevelFailures, boolean indexingPressureAccounted) implements Writeable {

        static final IncrementalState EMPTY = new IncrementalState(Collections.emptyMap(), false);

        IncrementalState(StreamInput in) throws IOException {
            this(in.readMap(ShardId::new, input -> input.readException()), false);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(shardLevelFailures, (o, s) -> s.writeTo(o), StreamOutput::writeException);
        }
    }

    /*
     * This copies this bulk request, but without all of its inner requests or the set of indices found in those requests
     */
    public BulkRequest shallowClone() {
        BulkRequest bulkRequest = new BulkRequest(globalIndex);
        bulkRequest.setRefreshPolicy(getRefreshPolicy());
        bulkRequest.waitForActiveShards(waitForActiveShards());
        bulkRequest.timeout(timeout());
        bulkRequest.pipeline(pipeline());
        bulkRequest.routing(routing());
        bulkRequest.requireAlias(requireAlias());
        bulkRequest.requireDataStream(requireDataStream());
        return bulkRequest;
    }
}
