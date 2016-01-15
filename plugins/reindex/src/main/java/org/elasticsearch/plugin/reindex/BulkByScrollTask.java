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

package org.elasticsearch.plugin.reindex;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.tasks.Task;

import static java.lang.Math.min;
import static java.util.Collections.unmodifiableList;

/**
 * Task storing information about a currently running BulkByScroll request.
 */
public class BulkByScrollTask extends Task {
    private final AtomicLong total = new AtomicLong(-1);
    private final AtomicLong updated = new AtomicLong(0);
    private final AtomicLong created = new AtomicLong(0);
    private final AtomicLong deleted = new AtomicLong(0);
    private final AtomicLong noops = new AtomicLong(0);
    private final AtomicInteger batch = new AtomicInteger(0);
    private final AtomicLong versionConflicts = new AtomicLong(0);
    private final List<Failure> indexingFailures = new CopyOnWriteArrayList<>();
    private final List<ShardSearchFailure> searchFailures = new CopyOnWriteArrayList<>();

    public BulkByScrollTask(long id, String type, String action) {
        super(id, type, action, null);
    }

    @Override
    public String toString() {
        long total = this.total.get();
        if (total < 0) {
            return "unstarted";
        }
        StringBuilder builder = new StringBuilder();
        builder.append(successfullyProcessed()).append('/').append(total);
        builder.append("[updated=").append(updated);
        builder.append(",created=").append(created);
        builder.append(",deleted=").append(deleted);
        builder.append(",batch=").append(batch);
        builder.append(",version_conflicts=").append(versionConflicts);
        builder.append(",noops=").append(noops);
        List<Failure> indexingFailures = this.indexingFailures.subList(0, min(3, this.indexingFailures.size()));
        if (false == indexingFailures.isEmpty()) {
            builder.append(",indexing_failures=").append(indexingFailures);
        }
        List<ShardSearchFailure> searchFailures = this.searchFailures.subList(0, min(3, this.searchFailures.size()));
        if (false == searchFailures.isEmpty()) {
            builder.append(",search_failures=").append(searchFailures);
        }
        return builder.append(']').toString();
    }

    @Override
    public String getDescription() {
        return toString();
    }

    /**
     * Count of documents updated.
     */
    public long updated() {
        return updated.get();
    }

    /**
     * Count of documents created.
     */
    public long created() {
        return created.get();
    }

    /**
     * Count of successful delete operations.
     */
    public long deleted() {
        return deleted.get();
    }

    /**
     * Number of scan responses this request has processed.
     */
    public int batches() {
        return batch.get();
    }

    /**
     * Number of noops (skipped bulk items) as part of this request.
     */
    public long noops() {
        return noops.get();
    }

    /**
     * Number of version conflicts this request has hit.
     */
    public long versionConflicts() {
        return versionConflicts.get();
    }

    /**
     * Total number of successfully processed documents.
     */
    public long successfullyProcessed() {
        return updated.get() + created.get() + deleted.get();
    }

    /**
     * All indexing failures.
     */
    public List<Failure> indexingFailures() {
        return unmodifiableList(indexingFailures);
    }

    /**
     * All search failures.
     */
    public List<ShardSearchFailure> searchFailures() {
        return unmodifiableList(searchFailures);
    }

    void addSearchFailures(ShardSearchFailure... shardSearchFailures) {
        Collections.addAll(searchFailures, shardSearchFailures);
    }

    void countBatch() {
        batch.incrementAndGet();
    }

    void countNoop() {
        noops.incrementAndGet();
    }

    void countCreated() {
        created.incrementAndGet();
    }

    void countUpdated() {
        updated.incrementAndGet();
    }

    void countDeleted() {
        deleted.incrementAndGet();
    }

    void countVersionConflict() {
        versionConflicts.incrementAndGet();
    }

    void addIndexingFailure(Failure failure) {
        indexingFailures.add(failure);
    }

    void setTotal(long totalHits) {
        total.set(totalHits);
    }
}
