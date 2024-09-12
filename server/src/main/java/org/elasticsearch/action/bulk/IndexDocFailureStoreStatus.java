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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

/**
 * Captures the role of the failure store in this document response. For example,
 * - USED, means that this document was stored in the failure store
 * - NOT_ENABLED, means that this document was rejected by elasticsearch, but it could have been stored in
 * the failure store has it been enabled.
 * - FAILED, means that this failed document was eligible to be stored in the failure store and the failure store
 * was enabled but something went wrong.
 * - NOT_APPLICABLE_OR_UNKNOWN, means either that we have no information about this response, for example, in a mixed state
 * cluster or the document wasn't eligible for the failure store, either because it was successfully stored in the data stream
 * or it failed to be indexed in an index (failure store is only supported in data streams).
 */
public enum IndexDocFailureStoreStatus implements ToXContentFragment, Writeable {
    NOT_APPLICABLE_OR_UNKNOWN(0),
    USED(1),
    NOT_ENABLED(2),
    FAILED(3);

    private static final Set<IndexDocFailureStoreStatus> forDisplay = Set.of(USED, NOT_ENABLED, FAILED);
    private final byte id;
    private final String label;

    IndexDocFailureStoreStatus(int id) {
        this.id = (byte) id;
        this.label = this.toString().toLowerCase(Locale.ROOT);
    }

    public static IndexDocFailureStoreStatus read(StreamInput in) throws IOException {
        return fromId(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id);
    }

    /**
     * @return id of the status, mainly used for wire serialisation purposes
     */
    public byte getId() {
        return id;
    }

    /**
     * @return the label of this status for display, just lowercase of the enum
     */
    public String getLabel() {
        return label;
    }

    /**
     * @param id a candidate id that (hopefully) can be converted to a FailureStoreStatus, used in wire serialisation
     * @return the failure store status that corresponds to the id.
     * @throws IllegalArgumentException when the id cannot produce a failure store status
     */
    public static IndexDocFailureStoreStatus fromId(byte id) {
        return switch (id) {
            case 0 -> NOT_APPLICABLE_OR_UNKNOWN;
            case 1 -> USED;
            case 2 -> NOT_ENABLED;
            case 3 -> FAILED;
            default -> throw new IllegalArgumentException("Unknown failure store status: [" + id + "]");
        };
    }

    /**
     * This helper function tried to determine if the failure store was used or not from the index name. This
     * should only be used in mixed cluster where the failure store status information is missing.
     *
     * @param response the response part of the BulkItemResponse.
     * @return <code>USED</code> if the doc was indexed in an index with the failure store prefix, <code>NOT_APPLICABLE_OR_UNKNOWN</code>
     * otherwise.
     */
    static IndexDocFailureStoreStatus inferFromResponse(@Nullable DocWriteResponse response) {
        if (response != null && response.getIndex().startsWith(DataStream.FAILURE_STORE_PREFIX)) {
            return USED;
        }
        return NOT_APPLICABLE_OR_UNKNOWN;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (forDisplay.contains(this)) {
            builder.field("failure_store", label);
        }
        return builder;
    }

    /**
     * Exception wrapper class that adds the failure store status in the XContent response.
     * Note: We are not using {@link ExceptionWrapper} because then it unwraps it directly in the {@link ElasticsearchException}
     * and we cannot add the field.
     */
    public static class ExceptionWrapper extends ElasticsearchException {

        private final IndexDocFailureStoreStatus failureStoreStatus;

        public ExceptionWrapper(BulkItemResponse.Failure failure) {
            super(failure.getCause());
            this.failureStoreStatus = failure.getFailureStoreStatus();
        }

        public ExceptionWrapper(StreamInput in) throws IOException {
            super(in);
            failureStoreStatus = IndexDocFailureStoreStatus.fromId(in.readByte());
        }

        @Override
        protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
            super.writeTo(out, nestedExceptionsWriter);
            out.writeByte(failureStoreStatus.getId());
        }

        @Override
        protected XContentBuilder toXContent(XContentBuilder builder, Params params, int nestedLevel) throws IOException {
            generateThrowableXContent(builder, params, this.getCause(), nestedLevel);
            failureStoreStatus.toXContent(builder, params);
            return builder;
        }

        @Override
        public RestStatus status() {
            return ExceptionsHelper.status(getCause());
        }
    }
}
