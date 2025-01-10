/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;

public class ReindexDataStreamIndexAction extends ActionType<ReindexDataStreamIndexAction.Response> {

    public static final String NAME = "indices:admin/data_stream/index/reindex";

    public static final ActionType<Response> INSTANCE = new ReindexDataStreamIndexAction();

    private ReindexDataStreamIndexAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest implements IndicesRequest {

        private final String sourceIndex;

        /**
         * The api blocks which were set on the source index before the reindex operation.
         * A `read-only` block is set on the source index before reindexing so that it is not
         * modified. Setting this block could require modifying other blocks, for example if the
         * index has a `metadata` block. For this reason, all block are removed before setting
         * the `read-only` block. The original blocks are restored on the destination index after
         * reindexing. If a reindex operation fails and is retried, the blocks on the source
         * at the start of this action may be different from the original blocks, thus all
         * blocks are stored in the task state, so they survive retries and can be copied to the
         * destination index.
         */
        private EnumSet<IndexMetadata.APIBlock> sourceBlocks;

        public Request(String sourceIndex, EnumSet<IndexMetadata.APIBlock> sourceBlocks) {
            this.sourceIndex = sourceIndex;
            this.sourceBlocks = sourceBlocks;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
            this.sourceBlocks = in.readEnumSet(IndexMetadata.APIBlock.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeEnumSet(sourceBlocks);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public EnumSet<IndexMetadata.APIBlock> getSourceBlocks() {
            return sourceBlocks;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(sourceIndex, request.sourceIndex) && Objects.equals(sourceBlocks, request.sourceBlocks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, sourceBlocks);
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }

    public static class Response extends ActionResponse {
        private final String destIndex;

        public Response(String destIndex) {
            this.destIndex = destIndex;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.destIndex = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(destIndex);
        }

        public String getDestIndex() {
            return destIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(destIndex, response.destIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(destIndex);
        }
    }
}
