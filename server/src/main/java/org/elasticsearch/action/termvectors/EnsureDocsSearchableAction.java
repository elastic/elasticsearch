/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 *
 * This file was contributed to by generative AI
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * This action is used in serverless to ensure that documents are searchable on the search tier before processing
 * term vector requests. It is an intermediate action that is executed on the indexing node and responds
 * with a no-op (the search node can proceed to process the term vector request). The action may trigger an external refresh
 * to ensure the search shards are up to date before returning the no-op.
 */
public class EnsureDocsSearchableAction {

    private static final String ACTION_NAME = "internal:index/data/read/eds";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(ACTION_NAME);
    public static final String ENSURE_DOCS_SEARCHABLE_ORIGIN = "ensure_docs_searchable";

    public static final class EnsureDocsSearchableRequest extends SingleShardRequest<EnsureDocsSearchableRequest> {

        private int shardId; // this is not serialized over the wire, and will be 0 on the other end of the wire.
        private String[] docIds;

        public EnsureDocsSearchableRequest() {}

        public EnsureDocsSearchableRequest(StreamInput in) throws IOException {
            super(in);
            docIds = in.readStringArray();
        }

        @Override
        public ActionRequestValidationException validate() {
            return super.validateNonNullIndex();
        }

        public EnsureDocsSearchableRequest(String index, int shardId, String[] docIds) {
            super(index);
            this.shardId = shardId;
            this.docIds = docIds;
        }

        public int shardId() {
            return this.shardId;
        }

        public String[] docIds() {
            return docIds;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(docIds);
        }

    }

}
