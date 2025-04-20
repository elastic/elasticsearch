/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rank.RankDocShardInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Shard level fetch base request. Holds all the info needed to execute a fetch.
 * Used with search scroll as the original request doesn't hold indices.
 */
public class ShardFetchRequest extends AbstractTransportRequest {

    private final ShardSearchContextId contextId;

    private final int[] docIds;

    @Nullable
    private final ScoreDoc lastEmittedDoc;

    public ShardFetchRequest(ShardSearchContextId contextId, List<Integer> docIds, ScoreDoc lastEmittedDoc) {
        this.contextId = contextId;
        this.docIds = docIds.stream().mapToInt(Integer::intValue).toArray();
        this.lastEmittedDoc = lastEmittedDoc;
    }

    public ShardFetchRequest(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        int size = in.readVInt();
        docIds = new int[size];
        for (int i = 0; i < size; i++) {
            docIds[i] = in.readVInt();
        }
        byte flag = in.readByte();
        if (flag == 1) {
            lastEmittedDoc = Lucene.readFieldDoc(in);
        } else if (flag == 2) {
            lastEmittedDoc = Lucene.readScoreDoc(in);
        } else if (flag != 0) {
            throw new IOException("Unknown flag: " + flag);
        } else {
            lastEmittedDoc = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
        out.writeVIntArray(docIds);
        if (lastEmittedDoc == null) {
            out.writeByte((byte) 0);
        } else if (lastEmittedDoc instanceof FieldDoc) {
            out.writeByte((byte) 1);
            Lucene.writeFieldDoc(out, (FieldDoc) lastEmittedDoc);
        } else {
            out.writeByte((byte) 2);
            Lucene.writeScoreDoc(out, lastEmittedDoc);
        }
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public int[] docIds() {
        return docIds;
    }

    public ScoreDoc lastEmittedDoc() {
        return lastEmittedDoc;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "id[" + contextId + "], size[" + docIds.length + "], lastEmittedDoc[" + lastEmittedDoc + "]";
    }

    @Nullable
    public ShardSearchRequest getShardSearchRequest() {
        return null;
    }

    @Nullable
    public RescoreDocIds getRescoreDocIds() {
        return null;
    }

    @Nullable
    public AggregatedDfs getAggregatedDfs() {
        return null;
    }

    @Nullable
    public RankDocShardInfo getRankDocks() {
        return null;
    }
}
