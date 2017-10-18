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

package org.elasticsearch.action.bulk;

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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A bulk request holds an ordered {@link IndexRequest}s, {@link DeleteRequest}s and {@link UpdateRequest}s
 * and allows to executes it in a single batch.
 *
 * Note that we only support refresh on the bulk request not per item.
 * @see org.elasticsearch.client.Client#bulk(BulkRequest)
 */
public class BulkRequest extends ActionRequest implements CompositeIndicesRequest, WriteRequest<BulkRequest> {
    private static final DeprecationLogger DEPRECATION_LOGGER =
        new DeprecationLogger(Loggers.getLogger(BulkRequest.class));

    private static final int REQUEST_OVERHEAD = 50;

    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField TYPE = new ParseField("_type");
    private static final ParseField ID = new ParseField("_id");
    private static final ParseField ROUTING = new ParseField("routing", "_routing");
    private static final ParseField PARENT = new ParseField("parent", "_parent");
    private static final ParseField OP_TYPE = new ParseField("op_type", "opType");
    private static final ParseField VERSION = new ParseField("version", "_version");
    private static final ParseField VERSION_TYPE = new ParseField("version_type", "_version_type", "_versionType", "versionType");
    private static final ParseField RETRY_ON_CONFLICT = new ParseField("retry_on_conflict", "_retry_on_conflict", "_retryOnConflict");
    private static final ParseField PIPELINE = new ParseField("pipeline");
    private static final ParseField FIELDS = new ParseField("fields");
    private static final ParseField SOURCE = new ParseField("_source");

    /**
     * Requests that are part of this request. It is only possible to add things that are both {@link ActionRequest}s and
     * {@link WriteRequest}s to this but java doesn't support syntax to declare that everything in the array has both types so we declare
     * the one with the least casts.
     */
    final List<DocWriteRequest> requests = new ArrayList<>();
    private final Set<String> indices = new HashSet<>();
    List<Object> payloads = null;

    protected TimeValue timeout = BulkShardRequest.DEFAULT_TIMEOUT;
    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;
    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;

    private long sizeInBytes = 0;

    public BulkRequest() {
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public BulkRequest add(DocWriteRequest... requests) {
        for (DocWriteRequest request : requests) {
            add(request, null);
        }
        return this;
    }

    public BulkRequest add(DocWriteRequest request) {
        return add(request, null);
    }

    /**
     * Add a request to the current BulkRequest.
     * @param request Request to add
     * @param payload Optional payload
     * @return the current bulk request
     */
    public BulkRequest add(DocWriteRequest request, @Nullable Object payload) {
        if (request instanceof IndexRequest) {
            add((IndexRequest) request, payload);
        } else if (request instanceof DeleteRequest) {
            add((DeleteRequest) request, payload);
        } else if (request instanceof UpdateRequest) {
            add((UpdateRequest) request, payload);
        } else {
            throw new IllegalArgumentException("No support for request [" + request + "]");
        }
        indices.add(request.index());
        return this;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public BulkRequest add(Iterable<DocWriteRequest> requests) {
        for (DocWriteRequest request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkRequest add(IndexRequest request) {
        return internalAdd(request, null);
    }

    public BulkRequest add(IndexRequest request, @Nullable Object payload) {
        return internalAdd(request, payload);
    }

    BulkRequest internalAdd(IndexRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        requests.add(request);
        addPayload(payload);
        // lack of source is validated in validate() method
        sizeInBytes += (request.source() != null ? request.source().length() : 0) + REQUEST_OVERHEAD;
        indices.add(request.index());
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public BulkRequest add(UpdateRequest request) {
        return internalAdd(request, null);
    }

    public BulkRequest add(UpdateRequest request, @Nullable Object payload) {
        return internalAdd(request, payload);
    }

    BulkRequest internalAdd(UpdateRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        requests.add(request);
        addPayload(payload);
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
        return add(request, null);
    }

    public BulkRequest add(DeleteRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        requests.add(request);
        addPayload(payload);
        sizeInBytes += REQUEST_OVERHEAD;
        indices.add(request.index());
        return this;
    }

    private void addPayload(Object payload) {
        if (payloads == null) {
            if (payload == null) {
                return;
            }
            payloads = new ArrayList<>(requests.size() + 10);
            // add requests#size-1 elements to the payloads if it null (we add for an *existing* request)
            for (int i = 1; i < requests.size(); i++) {
                payloads.add(null);
            }
        }
        payloads.add(payload);
    }

    /**
     * The list of requests in this bulk request.
     */
    public List<DocWriteRequest> requests() {
        return this.requests;
    }

    /**
     * The list of optional payloads associated with requests in the same order as the requests. Note, elements within
     * it might be null if no payload has been provided.
     * <p>
     * Note, if no payloads have been provided, this method will return null (as to conserve memory overhead).
     */
    @Nullable
    public List<Object> payloads() {
        return this.payloads;
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
        return add(data, from, length, null, null, xContentType);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType,
                           XContentType xContentType) throws IOException {
        return add(new BytesArray(data, from, length), defaultIndex, defaultType, xContentType);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType,
                           XContentType xContentType) throws IOException {
        return add(data, defaultIndex, defaultType, null, null, null, null, null, true, xContentType);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, boolean allowExplicitIndex,
                           XContentType xContentType) throws IOException {
        return add(data, defaultIndex, defaultType, null, null, null, null, null, allowExplicitIndex, xContentType);
    }

    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String
        defaultRouting, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSourceContext, @Nullable String
        defaultPipeline, @Nullable Object payload, boolean allowExplicitIndex, XContentType xContentType) throws IOException {
        XContent xContent = xContentType.xContent();
        int line = 0;
        int from = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            line++;

            // now parse the action
            // EMPTY is safe here because we never call namedObject
            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, data.slice(from, nextMarker - from))) {
                // move pointers
                from = nextMarker + 1;

                // Move to START_OBJECT
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected "
                        + XContentParser.Token.START_OBJECT + " but found [" + token + "]");
                }
                // Move to FIELD_NAME, that's the action
                token = parser.nextToken();
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected "
                        + XContentParser.Token.FIELD_NAME + " but found [" + token + "]");
                }
                String action = parser.currentName();

                String index = defaultIndex;
                String type = defaultType;
                String id = null;
                String routing = defaultRouting;
                String parent = null;
                FetchSourceContext fetchSourceContext = defaultFetchSourceContext;
                String[] fields = defaultFields;
                String opType = null;
                long version = Versions.MATCH_ANY;
                VersionType versionType = VersionType.INTERNAL;
                int retryOnConflict = 0;
                String pipeline = defaultPipeline;

                // at this stage, next token can either be END_OBJECT (and use default index and type, with auto generated id)
                // or START_OBJECT which will have another set of parameters
                token = parser.nextToken();

                if (token == XContentParser.Token.START_OBJECT) {
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (INDEX.match(currentFieldName)){
                                if (!allowExplicitIndex) {
                                    throw new IllegalArgumentException("explicit index in bulk is not allowed");
                                }
                                index = parser.text();
                            } else if (TYPE.match(currentFieldName)) {
                                type = parser.text();
                            } else if (ID.match(currentFieldName)) {
                                id = parser.text();
                            } else if (ROUTING.match(currentFieldName)) {
                                routing = parser.text();
                            } else if (PARENT.match(currentFieldName)) {
                                parent = parser.text();
                            } else if (OP_TYPE.match(currentFieldName)) {
                                opType = parser.text();
                            } else if (VERSION.match(currentFieldName)) {
                                version = parser.longValue();
                            } else if (VERSION_TYPE.match(currentFieldName)) {
                                versionType = VersionType.fromString(parser.text());
                            } else if (RETRY_ON_CONFLICT.match(currentFieldName)) {
                                retryOnConflict = parser.intValue();
                            } else if (PIPELINE.match(currentFieldName)) {
                                pipeline = parser.text();
                            } else if (FIELDS.match(currentFieldName)) {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains a simple value for parameter [fields] while a list is expected");
                            } else if (SOURCE.match(currentFieldName)) {
                                fetchSourceContext = FetchSourceContext.fromXContent(parser);
                            } else {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains an unknown parameter [" + currentFieldName + "]");
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if (FIELDS.match(currentFieldName)) {
                                DEPRECATION_LOGGER.deprecated("Deprecated field [fields] used, expected [_source] instead");
                                List<Object> values = parser.list();
                                fields = values.toArray(new String[values.size()]);
                            } else {
                                throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                            }
                        } else if (token == XContentParser.Token.START_OBJECT && SOURCE.match(currentFieldName)) {
                            fetchSourceContext = FetchSourceContext.fromXContent(parser);
                        } else if (token != XContentParser.Token.VALUE_NULL) {
                            throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                        }
                    }
                } else if (token != XContentParser.Token.END_OBJECT) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected " + XContentParser.Token.START_OBJECT
                            + " or " + XContentParser.Token.END_OBJECT + " but found [" + token + "]");
                }

                if ("delete".equals(action)) {
                    add(new DeleteRequest(index, type, id).routing(routing).parent(parent).version(version).versionType(versionType), payload);
                } else {
                    nextMarker = findNextMarker(marker, from, data, length);
                    if (nextMarker == -1) {
                        break;
                    }
                    line++;

                    // we use internalAdd so we don't fork here, this allows us not to copy over the big byte array to small chunks
                    // of index request.
                    if ("index".equals(action)) {
                        if (opType == null) {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).version(version).versionType(versionType)
                                    .setPipeline(pipeline)
                                    .source(sliceTrimmingCarriageReturn(data, from, nextMarker,xContentType), xContentType), payload);
                        } else {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).version(version).versionType(versionType)
                                    .create("create".equals(opType)).setPipeline(pipeline)
                                    .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType), payload);
                        }
                    } else if ("create".equals(action)) {
                        internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).version(version).versionType(versionType)
                                .create(true).setPipeline(pipeline)
                                .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType), payload);
                    } else if ("update".equals(action)) {
                        UpdateRequest updateRequest = new UpdateRequest(index, type, id).routing(routing).parent(parent).retryOnConflict(retryOnConflict)
                                .version(version).versionType(versionType)
                                .routing(routing)
                                .parent(parent);
                        // EMPTY is safe here because we never call namedObject
                        try (XContentParser sliceParser = xContent.createParser(NamedXContentRegistry.EMPTY,
                                                        sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType))) {
                            updateRequest.fromXContent(sliceParser);
                        }
                        if (fetchSourceContext != null) {
                            updateRequest.fetchSource(fetchSourceContext);
                        }
                        if (fields != null) {
                            updateRequest.fields(fields);
                        }

                        IndexRequest upsertRequest = updateRequest.upsertRequest();
                        if (upsertRequest != null) {
                            upsertRequest.version(version);
                            upsertRequest.versionType(versionType);
                            upsertRequest.setPipeline(defaultPipeline);
                        }
                        IndexRequest doc = updateRequest.doc();
                        if (doc != null) {
                            doc.version(version);
                            doc.versionType(versionType);
                        }

                        internalAdd(updateRequest, payload);
                    }
                    // move pointers
                    from = nextMarker + 1;
                }
            }
        }
        return this;
    }

    /**
     * Returns the sliced {@link BytesReference}. If the {@link XContentType} is JSON, the byte preceding the marker is checked to see
     * if it is a carriage return and if so, the BytesReference is sliced so that the carriage return is ignored
     */
    private BytesReference sliceTrimmingCarriageReturn(BytesReference bytesReference, int from, int nextMarker, XContentType xContentType) {
        final int length;
        if (XContentType.JSON == xContentType && bytesReference.get(nextMarker - 1) == (byte) '\r') {
            length = nextMarker - from - 1;
        } else {
            length = nextMarker - from;
        }
        return bytesReference.slice(from, length);
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
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final BulkRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final BulkRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public TimeValue timeout() {
        return timeout;
    }

    private int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        if (from != length) {
            throw new IllegalArgumentException("The bulk request must be terminated by a newline [\n]");
        }
        return -1;
    }

    /**
     * @return Whether this bulk request contains index request with an ingest pipeline enabled.
     */
    public boolean hasIndexRequestsWithPipelines() {
        for (DocWriteRequest actionRequest : requests) {
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                if (Strings.hasText(indexRequest.getPipeline())) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (DocWriteRequest request : requests) {
            // We first check if refresh has been set
            if (((WriteRequest<?>) request).getRefreshPolicy() != RefreshPolicy.NONE) {
                validationException = addValidationError(
                        "RefreshPolicy is not supported on an item request. Set it on the BulkRequest instead.", validationException);
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            requests.add(DocWriteRequest.readDocumentRequest(in));
        }
        refreshPolicy = RefreshPolicy.readFrom(in);
        timeout = new TimeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeVInt(requests.size());
        for (DocWriteRequest request : requests) {
            DocWriteRequest.writeDocumentRequest(out, request);
        }
        refreshPolicy.writeTo(out);
        timeout.writeTo(out);
    }

    @Override
    public String getDescription() {
        return "requests[" + requests.size() + "], indices[" + Strings.collectionToDelimitedString(indices, ", ") + "]";
    }

}
