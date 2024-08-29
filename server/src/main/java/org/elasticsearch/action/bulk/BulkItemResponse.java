/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents a single item response for an action executed as part of the bulk API. Holds the index/type/id
 * of the relevant action, and if it has failed or not (with the failure message in case it failed).
 */
public class BulkItemResponse implements Writeable, ToXContentObject {

    private static final String _INDEX = "_index";
    private static final String _ID = "_id";
    static final String STATUS = "status";
    static final String ERROR = "error";

    public RestStatus status() {
        return failure == null ? response.status() : failure.getStatus();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(opType.getLowercase());
        if (failure == null) {
            response.innerToXContent(builder, params);
            builder.field(STATUS, response.status().getStatus());
        } else {
            builder.field(_INDEX, failure.getIndex());
            if (builder.getRestApiVersion() == RestApiVersion.V_7) {
                builder.field(MapperService.TYPE_FIELD_NAME, MapperService.SINGLE_MAPPING_NAME);
            }

            builder.field(_ID, failure.getId());
            builder.field(STATUS, failure.getStatus().getStatus());
            builder.startObject(ERROR);
            ElasticsearchException.generateThrowableXContent(builder, params, failure.getCause());
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Represents a failure.
     */
    public static class Failure implements Writeable, ToXContentFragment {
        public static final String INDEX_FIELD = "index";
        public static final String ID_FIELD = "id";
        public static final String CAUSE_FIELD = "cause";
        public static final String STATUS_FIELD = "status";

        private final String index;
        private final String id;
        private final Exception cause;
        private final RestStatus status;
        private final long seqNo;
        private final long term;
        private final boolean aborted;

        /**
         * For write failures before operation was assigned a sequence number.
         *
         * use @{link {@link #Failure(String, String, Exception, long, long)}}
         * to record operation sequence no with failure
         */
        public Failure(String index, String id, Exception cause) {
            this(
                index,
                id,
                cause,
                ExceptionsHelper.status(cause),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                false
            );
        }

        public Failure(String index, String id, Exception cause, boolean aborted) {
            this(
                index,
                id,
                cause,
                ExceptionsHelper.status(cause),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                aborted
            );
        }

        public Failure(String index, String id, Exception cause, RestStatus status) {
            this(index, id, cause, status, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, false);
        }

        /** For write failures after operation was assigned a sequence number. */
        public Failure(String index, String id, Exception cause, long seqNo, long term) {
            this(index, id, cause, ExceptionsHelper.status(cause), seqNo, term, false);
        }

        private Failure(String index, String id, Exception cause, RestStatus status, long seqNo, long term, boolean aborted) {
            this.index = index;
            this.id = id;
            this.cause = cause;
            this.status = status;
            this.seqNo = seqNo;
            this.term = term;
            this.aborted = aborted;
        }

        /**
         * Read from a stream.
         */
        public Failure(StreamInput in) throws IOException {
            index = in.readString();
            if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
                in.readString();
                // can't make an assertion about type names here because too many tests still set their own
                // types bypassing various checks
            }
            id = in.readOptionalString();
            cause = in.readException();
            status = ExceptionsHelper.status(cause);
            seqNo = in.readZLong();
            term = in.readVLong();
            aborted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeOptionalString(id);
            out.writeException(cause);
            out.writeZLong(seqNo);
            out.writeVLong(term);
            out.writeBoolean(aborted);
        }

        /**
         * The index name of the action.
         */
        public String getIndex() {
            return this.index;
        }

        /**
         * The id of the action.
         */
        public String getId() {
            return id;
        }

        /**
         * The failure message.
         */
        public String getMessage() {
            return this.cause.toString();
        }

        /**
         * The rest status.
         */
        public RestStatus getStatus() {
            return this.status;
        }

        /**
         * The actual cause of the failure.
         */
        public Exception getCause() {
            return cause;
        }

        /**
         * The operation sequence number generated by primary
         * NOTE: {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
         * indicates sequence number was not generated by primary
         */
        public long getSeqNo() {
            return seqNo;
        }

        /**
         * The operation primary term of the primary
         * NOTE: {@link SequenceNumbers#UNASSIGNED_PRIMARY_TERM}
         * indicates primary term was not assigned by primary
         */
        public long getTerm() {
            return term;
        }

        /**
         * Whether this failure is the result of an <em>abort</em>.
         * If {@code true}, the request to which this failure relates should never be retried, regardless of the {@link #getCause() cause}.
         * @see BulkItemRequest#abort(String, Exception)
         */
        public boolean isAborted() {
            return aborted;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(INDEX_FIELD, index);
            if (builder.getRestApiVersion() == RestApiVersion.V_7) {
                builder.field("type", MapperService.SINGLE_MAPPING_NAME);
            }
            if (id != null) {
                builder.field(ID_FIELD, id);
            }
            builder.startObject(CAUSE_FIELD);
            ElasticsearchException.generateThrowableXContent(builder, params, cause);
            builder.endObject();
            builder.field(STATUS_FIELD, status.getStatus());
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    private final int id;

    private final OpType opType;

    private final DocWriteResponse response;

    private final Failure failure;

    BulkItemResponse(ShardId shardId, StreamInput in) throws IOException {
        id = in.readVInt();
        opType = OpType.fromId(in.readByte());
        response = readResponse(shardId, in);
        failure = in.readOptionalWriteable(Failure::new);
        assertConsistent();
    }

    BulkItemResponse(StreamInput in) throws IOException {
        id = in.readVInt();
        opType = OpType.fromId(in.readByte());
        response = readResponse(in);
        failure = in.readOptionalWriteable(Failure::new);
        assertConsistent();
    }

    private BulkItemResponse(int id, OpType opType, DocWriteResponse response, Failure failure) {
        this.id = id;
        this.response = response;
        this.opType = opType;
        this.failure = failure;
        assertConsistent();
    }

    private void assertConsistent() {
        assert (response == null) ^ (failure == null) : "only one of response or failure may be set";
    }

    public static BulkItemResponse success(int id, OpType opType, DocWriteResponse response) {
        return new BulkItemResponse(id, opType, response, null);
    }

    public static BulkItemResponse failure(int id, OpType opType, Failure failure) {
        return new BulkItemResponse(id, opType, null, failure);
    }

    /**
     * The numeric order of the item matching the same request order in the bulk request.
     */
    public int getItemId() {
        return id;
    }

    /**
     * The operation type ("index", "create" or "delete").
     */
    public OpType getOpType() {
        return this.opType;
    }

    /**
     * The index name of the action.
     */
    public String getIndex() {
        if (failure != null) {
            return failure.getIndex();
        }
        return response.getIndex();
    }

    /**
     * The id of the action.
     */
    public String getId() {
        if (failure != null) {
            return failure.getId();
        }
        return response.getId();
    }

    /**
     * The version of the action.
     */
    public long getVersion() {
        if (failure != null) {
            return -1;
        }
        return response.getVersion();
    }

    /**
     * The actual response ({@link IndexResponse} or {@link DeleteResponse}). {@code null} in
     * case of failure.
     */
    @SuppressWarnings("unchecked")
    public <T extends DocWriteResponse> T getResponse() {
        return (T) response;
    }

    /**
     * Is this a failed execution of an operation.
     */
    public boolean isFailed() {
        return failure != null;
    }

    /**
     * The failure message, {@code null} if it did not fail.
     */
    public String getFailureMessage() {
        if (failure != null) {
            return failure.getMessage();
        }
        return null;
    }

    /**
     * The actual failure object if there was a failure.
     */
    public Failure getFailure() {
        return this.failure;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeByte(opType.getId());

        if (response == null) {
            out.writeByte((byte) 2);
        } else {
            writeResponseType(out);
            response.writeTo(out);
        }
        out.writeOptionalWriteable(failure);
    }

    public static final Writer<BulkItemResponse> THIN_WRITER = (out, item) -> {
        out.writeVInt(item.id);
        out.writeByte(item.opType.getId());

        if (item.response == null) {
            out.writeByte((byte) 2);
        } else {
            item.writeResponseType(out);
            item.response.writeThin(out);
        }
        out.writeOptionalWriteable(item.failure);
    };

    private void writeResponseType(StreamOutput out) throws IOException {
        if (response instanceof SimulateIndexResponse) {
            out.writeByte((byte) 4);
        } else if (response instanceof IndexResponse) {
            out.writeByte((byte) 0);
        } else if (response instanceof DeleteResponse) {
            out.writeByte((byte) 1);
        } else if (response instanceof UpdateResponse) {
            out.writeByte((byte) 3); // make 3 instead of 2, because 2 is already in use for 'no responses'
        } else {
            throw new IllegalStateException("Unexpected response type found [" + response.getClass() + "]");
        }
    }

    private static DocWriteResponse readResponse(ShardId shardId, StreamInput in) throws IOException {
        int type = in.readByte();
        return switch (type) {
            case 0 -> new IndexResponse(shardId, in);
            case 1 -> new DeleteResponse(shardId, in);
            case 2 -> null;
            case 3 -> new UpdateResponse(shardId, in);
            case 4 -> new SimulateIndexResponse(in);
            default -> throw new IllegalArgumentException("Unexpected type [" + type + "]");
        };
    }

    private static DocWriteResponse readResponse(StreamInput in) throws IOException {
        int type = in.readByte();
        return switch (type) {
            case 0 -> new IndexResponse(in);
            case 1 -> new DeleteResponse(in);
            case 2 -> null;
            case 3 -> new UpdateResponse(in);
            case 4 -> new SimulateIndexResponse(in);
            default -> throw new IllegalArgumentException("Unexpected type [" + type + "]");
        };
    }
}
