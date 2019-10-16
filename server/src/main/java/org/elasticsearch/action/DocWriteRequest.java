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
package org.elasticsearch.action;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Generic interface to group ActionRequest, which perform writes to a single document
 * Action requests implementing this can be part of {@link org.elasticsearch.action.bulk.BulkRequest}
 */
public interface DocWriteRequest<T> extends IndicesRequest {

    /**
     * Set the index for this request
     * @return the Request
     */
    T index(String index);

    /**
     * Get the index that this request operates on
     * @return the index
     */
    String index();

    /**
     * Get the id of the document for this request
     * @return the id
     */
    String id();

    /**
     * Get the options for this request
     * @return the indices options
     */
    @Override
    IndicesOptions indicesOptions();

    /**
     * Set the routing for this request
     * @return the Request
     */
    T routing(String routing);

    /**
     * Get the routing for this request
     * @return the Routing
     */
    String routing();

    /**
     * Get the document version for this request
     * @return the document version
     */
    long version();

    /**
     * Sets the version, which will perform the operation only if a matching
     * version exists and no changes happened on the doc since then.
     */
    T version(long version);

    /**
     * Get the document version type for this request
     * @return the document version type
     */
    VersionType versionType();

    /**
     * Sets the versioning type. Defaults to {@link VersionType#INTERNAL}.
     */
    T versionType(VersionType versionType);

    /**
     * only perform this request if the document was last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    T setIfSeqNo(long seqNo);

    /**
     * only performs this request if the document was last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    T setIfPrimaryTerm(long term);

    /**
     * If set, only perform this request if the document was last modification was assigned this sequence number.
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
     long ifSeqNo();

    /**
     * If set, only perform this request if the document was last modification was assigned this primary term.
     *
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    long ifPrimaryTerm();

    /**
     * Get the requested document operation type of the request
     * @return the operation type {@link OpType}
     */
    OpType opType();

    /**
     * Requested operation type to perform on the document
     */
    enum OpType {
        /**
         * Index the source. If there an existing document with the id, it will
         * be replaced.
         */
        INDEX(0),
        /**
         * Creates the resource. Simply adds it to the index, if there is an existing
         * document with the id, then it won't be removed.
         */
        CREATE(1),
        /** Updates a document */
        UPDATE(2),
        /** Deletes a document */
        DELETE(3);

        private final byte op;
        private final String lowercase;

        OpType(int op) {
            this.op = (byte) op;
            this.lowercase = this.toString().toLowerCase(Locale.ROOT);
        }

        public byte getId() {
            return op;
        }

        public String getLowercase() {
            return lowercase;
        }

        public static OpType fromId(byte id) {
            switch (id) {
                case 0: return INDEX;
                case 1: return CREATE;
                case 2: return UPDATE;
                case 3: return DELETE;
                default: throw new IllegalArgumentException("Unknown opType: [" + id + "]");
            }
        }

        public static OpType fromString(String sOpType) {
            String lowerCase = sOpType.toLowerCase(Locale.ROOT);
            for (OpType opType : OpType.values()) {
                if (opType.getLowercase().equals(lowerCase)) {
                    return opType;
                }
            }
            throw new IllegalArgumentException("Unknown opType: [" + sOpType + "]");
        }
    }

    /** read a document write (index/delete/update) request */
    static DocWriteRequest<?> readDocumentRequest(StreamInput in) throws IOException {
        byte type = in.readByte();
        DocWriteRequest<?> docWriteRequest;
        if (type == 0) {
            docWriteRequest = new IndexRequest(in);
        } else if (type == 1) {
            docWriteRequest = new DeleteRequest(in);
        } else if (type == 2) {
            docWriteRequest = new UpdateRequest(in);
        } else {
            throw new IllegalStateException("invalid request type [" + type+ " ]");
        }
        return docWriteRequest;
    }

    /** write a document write (index/delete/update) request*/
    static void writeDocumentRequest(StreamOutput out, DocWriteRequest<?> request)  throws IOException {
        if (request instanceof IndexRequest) {
            out.writeByte((byte) 0);
            ((IndexRequest) request).writeTo(out);
        } else if (request instanceof DeleteRequest) {
            out.writeByte((byte) 1);
            ((DeleteRequest) request).writeTo(out);
        } else if (request instanceof UpdateRequest) {
            out.writeByte((byte) 2);
            ((UpdateRequest) request).writeTo(out);
        } else {
            throw new IllegalStateException("invalid request [" + request.getClass().getSimpleName() + " ]");
        }
    }

    static ActionRequestValidationException validateSeqNoBasedCASParams(
        DocWriteRequest request, ActionRequestValidationException validationException) {
        final long version = request.version();
        final VersionType versionType = request.versionType();
        if (versionType.validateVersionForWrites(version) == false) {
            validationException = addValidationError("illegal version value [" + version + "] for version type ["
                + versionType.name() + "]", validationException);
        }

        if (versionType == VersionType.INTERNAL && version != Versions.MATCH_ANY && version != Versions.MATCH_DELETED) {
            validationException = addValidationError("internal versioning can not be used for optimistic concurrency control. " +
                "Please use `if_seq_no` and `if_primary_term` instead", validationException);
        }

        if (request.ifSeqNo() != UNASSIGNED_SEQ_NO && (
            versionType != VersionType.INTERNAL || version != Versions.MATCH_ANY
        )) {
            validationException = addValidationError("compare and write operations can not use versioning", validationException);
        }
        if (request.ifPrimaryTerm() == UNASSIGNED_PRIMARY_TERM && request.ifSeqNo() != UNASSIGNED_SEQ_NO) {
            validationException = addValidationError("ifSeqNo is set, but primary term is [0]", validationException);
        }
        if (request.ifPrimaryTerm() != UNASSIGNED_PRIMARY_TERM && request.ifSeqNo() == UNASSIGNED_SEQ_NO) {
            validationException =
                addValidationError("ifSeqNo is unassigned, but primary term is [" + request.ifPrimaryTerm() + "]", validationException);
        }

        return validationException;
    }
}
