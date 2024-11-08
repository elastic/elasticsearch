/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Task storing information about a currently running {@link SearchRequest}.
 */
public class SearchTask extends CancellableTask {
    // generating description in a lazy way since source can be quite big
    private final Supplier<String> descriptionSupplier;
    private SearchProgressListener progressListener = SearchProgressListener.NOOP;
    private Supplier<SearchResponseMerger> searchResponseMergerSupplier;  // used for CCS minimize_roundtrips=true

    public SearchTask(
        long id,
        String type,
        String action,
        Supplier<String> descriptionSupplier,
        TaskId parentTaskId,
        Map<String, String> headers
    ) {
        super(id, type, action, null, parentTaskId, headers);
        this.descriptionSupplier = descriptionSupplier;
    }

    @Override
    public final String getDescription() {
        return descriptionSupplier.get();
    }

    /**
     * Attach a {@link SearchProgressListener} to this task.
     */
    public final void setProgressListener(SearchProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    /**
     * Return the {@link SearchProgressListener} attached to this task.
     */
    public final SearchProgressListener getProgressListener() {
        return progressListener;
    }

    /**
     * @return the Supplier of {@link SearchResponseMerger} attached to this task. Will be null
     * for local-only search and cross-cluster searches with minimize_roundtrips=false.
     */
    public Supplier<SearchResponseMerger> getSearchResponseMergerSupplier() {
        return searchResponseMergerSupplier;
    }

    /**
     * @param supplier Attach a Supplier of {@link SearchResponseMerger} to this task.
     *                 For use with CCS minimize_roundtrips=true
     */
    public void setSearchResponseMergerSupplier(Supplier<SearchResponseMerger> supplier) {
        this.searchResponseMergerSupplier = supplier;
    }

    /**
     * Is this async search?
     */
    public boolean isAsync() {
        return false;
    }
}
