/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Captures the role of the failure store in this document response. For example,
 * - USED, means that this document was stored in the failure store
 * - NOT_ENABLED, means that this document was rejected by elasticsearch, but it could have been stored in
 * the failure store has it been enabled.
 * - FAILED, means that this failed document was eligible to be stored in the failure store and the failure store
 * was enabled but something went wrong.
 */
public enum IndexDocFailureStoreStatus implements ToXContentFragment, Writeable {
    /**
     * This status represents that we have no information about this response or that the failure store is not applicable.
     * For example:
     * - when the doc was successfully indexed in a backing index of a data stream,
     * - when we are running in a mixed version cluster and the information is not available,
     * - when the doc was rejected by elasticsearch but failure store was not applicable (i.e. the target was an index).
     */
    NOT_APPLICABLE_OR_UNKNOWN(0),
    /**
     * This status represents that this document was stored in the failure store successfully.
     */
    USED(1),
    /**
     * This status represents that this document was rejected, but it could have ended up in the failure store if it was enabled.
     */
    NOT_ENABLED(2),
    /**
     * This status represents that this document was rejected from the failure store.
     */
    FAILED(3);

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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // We avoid adding the not_applicable status in the response to not increase the size of bulk responses.
        if (DataStream.isFailureStoreFeatureFlagEnabled() && this.equals(NOT_APPLICABLE_OR_UNKNOWN) == false) {
            builder.field("failure_store", label);
        }
        return builder;
    }

    /**
     * Exception wrapper class that adds the failure store status in the XContent response.
     * Note: We are not using {@link ExceptionWithFailureStoreStatus} because then it unwraps it directly
     * in the {@link ElasticsearchException} and we cannot add the field.
     */
    public static class ExceptionWithFailureStoreStatus extends ElasticsearchException {

        private final IndexDocFailureStoreStatus failureStoreStatus;

        public ExceptionWithFailureStoreStatus(BulkItemResponse.Failure failure) {
            super(failure.getCause());
            this.failureStoreStatus = failure.getFailureStoreStatus();
        }

        public ExceptionWithFailureStoreStatus(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                failureStoreStatus = IndexDocFailureStoreStatus.fromId(in.readByte());
            } else {
                failureStoreStatus = NOT_APPLICABLE_OR_UNKNOWN;
            }
        }

        @Override
        protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
            super.writeTo(out, nestedExceptionsWriter);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeByte(failureStoreStatus.getId());
            }
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
