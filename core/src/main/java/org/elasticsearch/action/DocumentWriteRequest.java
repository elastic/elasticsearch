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
import org.elasticsearch.action.update.UpdateReplicaRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.index.VersionType;

import java.io.IOException;
import java.util.Locale;

/**
 * Generic interface to group ActionRequest, which perform writes to a single document
 * Action requests implementing this can be part of {@link org.elasticsearch.action.bulk.BulkRequest}
 */
public abstract class DocumentWriteRequest<T extends ReplicatedWriteRequest<T>> extends ReplicatedWriteRequest<T> {

    /**
     * Get the type that this request operates on
     * @return the type
     */
    public abstract String type();

    /**
     * Get the id of the document for this request
     * @return the id
     */
    public abstract String id();

    /**
     * Set the routing for this request
     * @return the Request
     */
    public abstract T routing(String routing);

    /**
     * Get the routing for this request
     * @return the Routing
     */
    public abstract String routing();


    /**
     * Get the parent for this request
     * @return the Parent
     */
    public abstract String parent();

    /**
     * Get the document version for this request
     * @return the document version
     */
    public abstract long version();

    /**
     * Sets the version, which will perform the operation only if a matching
     * version exists and no changes happened on the doc since then.
     */
    public abstract T version(long version);

    /**
     * Get the document version type for this request
     * @return the document version type
     */
    public abstract VersionType versionType();

    /**
     * Sets the versioning type. Defaults to {@link VersionType#INTERNAL}.
     */
    public abstract T versionType(VersionType versionType);

    /**
     * Get the requested document operation type of the request
     * @return the operation type {@link OpType}
     */
    public abstract OpType opType();

    /**
     * Requested operation type to perform on the document
     */
    public enum OpType {
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
    public static DocumentWriteRequest readDocumentRequest(StreamInput in) throws IOException {
        byte type = in.readByte();
        if (type == 0) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.readFrom(in);
            return indexRequest;
        } else if (type == 1) {
            DeleteRequest deleteRequest = new DeleteRequest();
            deleteRequest.readFrom(in);
            return deleteRequest;
        } else if (type == 2) {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.readFrom(in);
            return updateRequest;
        } else if (type == 3) {
            UpdateReplicaRequest updateReplicaRequest = new UpdateReplicaRequest();
            updateReplicaRequest.readFrom(in);
            return updateReplicaRequest;
        } else {
            throw new IllegalStateException("invalid request type [" + type+ " ]");
        }
    }

    /** write a document write (index/delete/update) request*/
    public static void writeDocumentRequest(StreamOutput out, DocumentWriteRequest request)  throws IOException {
        if (request instanceof IndexRequest) {
            out.writeByte((byte) 0);
        } else if (request instanceof DeleteRequest) {
            out.writeByte((byte) 1);
        } else if (request instanceof UpdateRequest) {
            out.writeByte((byte) 2);
        } else if (request instanceof UpdateReplicaRequest) {
            out.writeByte((byte) 3);
        } else {
            throw new IllegalStateException("invalid request [" + request.getClass().getSimpleName() + " ]");
        }
        request.writeTo(out);
    }
}
