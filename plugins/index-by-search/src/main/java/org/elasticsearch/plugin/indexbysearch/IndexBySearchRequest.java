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

package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;

public class IndexBySearchRequest extends AbstractBulkByScrollRequest<IndexBySearchRequest> {
    /**
     * Prototype for index requests.
     *
     * Note that we co-opt version = Versions.NOT_SET to mean
     * "do not set the version in the index requests that we send for each scroll hit."
     */
    private IndexRequest index;

    private OpType opType = OpType.REFRESH;

    public IndexBySearchRequest() {
    }

    public IndexBySearchRequest(SearchRequest search, IndexRequest index) {
        super(search);
        this.index = index;

        // Clear the versionType so we can check if we've parsed it
        index.versionType(null);
    }

    @Override
    protected IndexBySearchRequest self() {
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        /*
         * Note that we don't validate the index here - it won't work because
         * we'll be filling in portions of it as we receive the docs. But we can
         * validate some things.
         */
        if (index.index() == null) {
            e = addValidationError("index must be specified", e);
            return e;
        }
        if (false == (index.routing() == null || index.routing().startsWith("=") ||
                "keep".equals(index.routing()) || "discard".equals(index.routing()))) {
            e = addValidationError("routing must be unset, [keep], [discard] or [=<some new value>]", e);
        }
        if (index.version() != Versions.MATCH_ANY) {
            e = addValidationError("overwriting version not supported but was [" + index.version() + ']', e);
        }
        if (index.versionType() != null) {
            e = addValidationError("overwriting version_type not supported but was [" + index.versionType() + ']', e);
        }
        return e;
    }

    public IndexRequest index() {
        return index;
    }

    public IndexBySearchRequest opType(OpType opType) {
        this.opType = opType;
        return this;
    }

    public OpType opType() {
        return opType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index.readFrom(in);
        opType = opType.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        index.writeTo(out);
        opType.writeTo(out);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("index-by-search from ");
        searchToString(b);
        b.append(" to [").append(index.index()).append(']');
        if (index.type() != null) {
            b.append('[').append(index.type()).append(']');
        }
        return b.toString();
    }

    /**
     * Controls how versions are handled during the index-by-search.
     */
    public static enum OpType implements Writeable<OpType> {
        OVERWRITE(0),
        CREATE(1),
        REFRESH(2);

        /**
         * Convenient prototype to call readFrom on.
         */
        public static final OpType PROTOTYPE = OVERWRITE;

        private byte id;

        private OpType(int id) {
            this.id = (byte) id;
        }

        @Override
        public OpType readFrom(StreamInput in) throws IOException {
            return fromId(in.readByte());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.write(id);
        }

        public static OpType fromString(String opType) {
            switch (opType) {
            case "overwrite":
                return OVERWRITE;
            case "create":
                return CREATE;
            case "refresh":
                return REFRESH;
            default:
                throw new IllegalArgumentException(
                        "OpType must be one of \"overwrite\", \"create\", or \"refresh\" but was [" + opType + ']');
            }
        }

        public static OpType fromId(byte id) {
            switch (id) {
            case 0:
                return OVERWRITE;
            case 1:
                return CREATE;
            case 2:
                return REFRESH;
            default:
                throw new IllegalArgumentException("OpType ids vary from 0 to 2 inclusive by was [" + id + ']');
            }
        }
    }
}
