/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;

import java.util.Arrays;
import java.util.Objects;

/**
 * This wrapper exists only to provide equals and hashCode implementations of a ForceMergeRequest for transportActionsDeduplicator.
 * It intentionally ignores forceMergeUUID (which ForceMergeRequest's equals/hashCode would have to if they existed) because we don't
 * care about it for data stream lifecycle deduplication. This class is public so that it can be reused by other data stream lifecycle
 * management components (for example when forming DLM force-merge requests), but it is not intended as a
 * general-purpose Elasticsearch public API.
 */
public final class ForceMergeRequestWrapper extends ForceMergeRequest {
    public ForceMergeRequestWrapper(ForceMergeRequest original) {
        super(original.indices());
        this.maxNumSegments(original.maxNumSegments());
        this.onlyExpungeDeletes(original.onlyExpungeDeletes());
        this.flush(original.flush());
        this.indicesOptions(original.indicesOptions());
        this.setShouldStoreResult(original.getShouldStoreResult());
        this.setRequestId(original.getRequestId());
        this.timeout(original.timeout());
        this.setParentTask(original.getParentTask());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForceMergeRequest that = (ForceMergeRequest) o;
        return Arrays.equals(indices, that.indices())
            && maxNumSegments() == that.maxNumSegments()
            && onlyExpungeDeletes() == that.onlyExpungeDeletes()
            && flush() == that.flush()
            && Objects.equals(indicesOptions(), that.indicesOptions())
            && getShouldStoreResult() == that.getShouldStoreResult()
            && getRequestId() == that.getRequestId()
            && Objects.equals(timeout(), that.timeout())
            && Objects.equals(getParentTask(), that.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            Arrays.hashCode(indices),
            maxNumSegments(),
            onlyExpungeDeletes(),
            flush(),
            indicesOptions(),
            getShouldStoreResult(),
            getRequestId(),
            timeout(),
            getParentTask()
        );
    }
}
