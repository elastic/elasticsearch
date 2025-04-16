/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IndexingStats;

import java.io.IOException;

public final class IndexStats implements Writeable {
    int numIndices = 0;
    long numDocs = 0;
    long numBytes = 0;
    SearchStats.Stats search = new SearchStats().getTotal();
    IndexingStats.Stats indexing = new IndexingStats().getTotal();

    IndexStats() {

    }

    IndexStats(StreamInput in) throws IOException {
        this.numIndices = in.readVInt();
        this.numDocs = in.readVLong();
        this.numBytes = in.readVLong();
        this.search = SearchStats.Stats.readStats(in);
        this.indexing = new IndexingStats.Stats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numIndices);
        out.writeVLong(numDocs);
        out.writeVLong(numBytes);
        search.writeTo(out);
        indexing.writeTo(out);
    }

    void add(IndexStats other) {
        this.numIndices += other.numIndices;
        this.numDocs += other.numDocs;
        this.numBytes += other.numBytes;
        this.search.add(other.search);
        this.indexing.add(other.indexing);
    }

    public int numIndices() {
        return numIndices;
    }

    public long numDocs() {
        return numDocs;
    }

    public long numBytes() {
        return numBytes;
    }
}
