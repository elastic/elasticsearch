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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;

/**
 * Represents a single item response for an action executed as part of the bulk API. Holds the index/type/id
 * of the relevant action, and if it has failed or not (with the failure message incase it failed).
 */
public class BulkItemResponse implements Streamable, StatusToXContentObject {

    private static final String _INDEX = "_index";
    private static final String _TYPE = "_type";
    private static final String _ID = "_id";
    private static final String STATUS = "status";
    private static final String ERROR = "error";

    @Override
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
            builder.field(_TYPE, failure.getType());
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
     * Reads a {@link BulkItemResponse} from a {@link XContentParser}.
     *
     * @param parser the {@link XContentParser}
     * @param id the id to assign to the parsed {@link BulkItemResponse}. It is usually the index of
     *           the item in the {@link BulkResponse#getItems} array.
     */
    public static BulkItemResponse fromXContent(XContentParser parser, int id) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);

        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);

        String currentFieldName = parser.currentName();
        token = parser.nextToken();

        final OpType opType = OpType.fromString(currentFieldName);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);

        DocWriteResponse.Builder builder = null;
        CheckedConsumer<XContentParser, IOException> itemParser = null;

        if (opType == OpType.INDEX || opType == OpType.CREATE) {
            final IndexResponse.Builder indexResponseBuilder = new IndexResponse.Builder();
            builder = indexResponseBuilder;
            itemParser = (indexParser) -> IndexResponse.parseXContentFields(indexParser, indexResponseBuilder);

        } else if (opType == OpType.UPDATE) {
            final UpdateResponse.Builder updateResponseBuilder = new UpdateResponse.Builder();
            builder = updateResponseBuilder;
            itemParser = (updateParser) -> UpdateResponse.parseXContentFields(updateParser, updateResponseBuilder);

        } else if (opType == OpType.DELETE) {
            final DeleteResponse.Builder deleteResponseBuilder = new DeleteResponse.Builder();
            builder = deleteResponseBuilder;
            itemParser = (deleteParser) -> DeleteResponse.parseXContentFields(deleteParser, deleteResponseBuilder);
        } else {
            throwUnknownField(currentFieldName, parser.getTokenLocation());
        }

        RestStatus status = null;
        ElasticsearchException exception = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }

            if (ERROR.equals(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    exception = ElasticsearchException.fromXContent(parser);
                }
            } else if (STATUS.equals(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    status = RestStatus.fromCode(parser.intValue());
                }
            } else {
                itemParser.accept(parser);
            }
        }

        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser::getTokenLocation);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser::getTokenLocation);

        BulkItemResponse bulkItemResponse;
        if (exception != null) {
            Failure failure = new Failure(builder.getShardId().getIndexName(), builder.getType(), builder.getId(), exception, status);
            bulkItemResponse = new BulkItemResponse(id, opType, failure);
        } else {
            bulkItemResponse = new BulkItemResponse(id, opType, builder.build());
        }
        return bulkItemResponse;
    }

    /**
     * Represents a failure.
     */
    public static class Failure implements Writeable, ToXContentFragment {
        static final String INDEX_FIELD = "index";
        static final String TYPE_FIELD = "type";
        static final String ID_FIELD = "id";
        static final String CAUSE_FIELD = "cause";
        static final String STATUS_FIELD = "status";

        private final String index;
        private final String type;
        private final String id;
        private final Exception cause;
        private final RestStatus status;
        private final long seqNo;
        private final boolean aborted;

        /**
         * For write failures before operation was assigned a sequence number.
         *
         * use @{link {@link #Failure(String, String, String, Exception, long)}}
         * to record operation sequence no with failure
         */
        public Failure(String index, String type, String id, Exception cause) {
            this(index, type, id, cause, ExceptionsHelper.status(cause), SequenceNumbers.UNASSIGNED_SEQ_NO, false);
        }

        public Failure(String index, String type, String id, Exception cause, boolean aborted) {
            this(index, type, id, cause, ExceptionsHelper.status(cause), SequenceNumbers.UNASSIGNED_SEQ_NO, aborted);
        }

        public Failure(String index, String type, String id, Exception cause, RestStatus status) {
            this(index, type, id, cause, status, SequenceNumbers.UNASSIGNED_SEQ_NO, false);
        }

        /** For write failures after operation was assigned a sequence number. */
        public Failure(String index, String type, String id, Exception cause, long seqNo) {
            this(index, type, id, cause, ExceptionsHelper.status(cause), seqNo, false);
        }

        public Failure(String index, String type, String id, Exception cause, RestStatus status, long seqNo, boolean aborted) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.cause = cause;
            this.status = status;
            this.seqNo = seqNo;
            this.aborted = aborted;
        }

        /**
         * Read from a stream.
         */
        public Failure(StreamInput in) throws IOException {
            index = in.readString();
            type = in.readString();
            id = in.readOptionalString();
            cause = in.readException();
            status = ExceptionsHelper.status(cause);
            if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
                seqNo = in.readZLong();
            } else {
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            }
            if (supportsAbortedFlag(in.getVersion())) {
                aborted = in.readBoolean();
            } else {
                aborted = false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getIndex());
            out.writeString(getType());
            out.writeOptionalString(getId());
            out.writeException(getCause());
            if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
                out.writeZLong(getSeqNo());
            }
            if (supportsAbortedFlag(out.getVersion())) {
                out.writeBoolean(aborted);
            }
        }

        private static boolean supportsAbortedFlag(Version version) {
            // The "aborted" flag was added for 5.5.3 and 5.6.0, but was not in 6.0.0-beta2
            return version.after(Version.V_6_0_0_beta2) || (version.major == 5 && version.onOrAfter(Version.V_5_5_3));
        }

        /**
         * The index name of the action.
         */
        public String getIndex() {
            return this.index;
        }

        /**
         * The type of the action.
         */
        public String getType() {
            return type;
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
            builder.field(TYPE_FIELD, type);
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

    private int id;

    private OpType opType;

    private DocWriteResponse response;

    private Failure failure;

    BulkItemResponse() {

    }

    public BulkItemResponse(int id, OpType opType, DocWriteResponse response) {
        this.id = id;
        this.response = response;
        this.opType = opType;
    }

    public BulkItemResponse(int id, OpType opType, Failure failure) {
        this.id = id;
        this.opType = opType;
        this.failure = failure;
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
     * The type of the action.
     */
    public String getType() {
        if (failure != null) {
            return failure.getType();
        }
        return response.getType();
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
     * The actual response ({@link IndexResponse} or {@link DeleteResponse}). <tt>null</tt> in
     * case of failure.
     */
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
     * The failure message, <tt>null</tt> if it did not fail.
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

    public static BulkItemResponse readBulkItem(StreamInput in) throws IOException {
        BulkItemResponse response = new BulkItemResponse();
        response.readFrom(in);
        return response;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            opType = OpType.fromId(in.readByte());
        } else {
            opType = OpType.fromString(in.readString());
        }

        byte type = in.readByte();
        if (type == 0) {
            response = new IndexResponse();
            response.readFrom(in);
        } else if (type == 1) {
            response = new DeleteResponse();
            response.readFrom(in);

        } else if (type == 3) { // make 3 instead of 2, because 2 is already in use for 'no responses'
            response = new UpdateResponse();
            response.readFrom(in);
        }

        if (in.readBoolean()) {
            failure = new Failure(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            out.writeByte(opType.getId());
        } else {
            out.writeString(opType.getLowercase());
        }

        if (response == null) {
            out.writeByte((byte) 2);
        } else {
            if (response instanceof IndexResponse) {
                out.writeByte((byte) 0);
            } else if (response instanceof DeleteResponse) {
                out.writeByte((byte) 1);
            } else if (response instanceof UpdateResponse) {
                out.writeByte((byte) 3); // make 3 instead of 2, because 2 is already in use for 'no responses'
            }
            response.writeTo(out);
        }
        if (failure == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            failure.writeTo(out);
        }
    }
}
