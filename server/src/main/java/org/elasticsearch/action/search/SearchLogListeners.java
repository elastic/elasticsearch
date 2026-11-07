/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.logging.activity.ActivityLogListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.tasks.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * The listeners that plugins give to follow searches through the query log (see {@link ActionPlugin#getSearchLogListeners()}).
 */
public final class SearchLogListeners {

    private static final Logger logger = LogManager.getLogger(SearchLogListeners.class);

    public static final SearchLogListeners NONE = new SearchLogListeners(List.of());

    private final List<ActivityLogListener<SearchLogContext, SearchRequest>> listeners;

    public SearchLogListeners(List<ActivityLogListener<SearchLogContext, SearchRequest>> listeners) {
        this.listeners = List.copyOf(listeners);
    }

    /**
     * Called when a search starts, for every search, so nothing is allocated unless a listener follows it.
     *
     * @return what the listeners that follow this search want called once it completes
     */
    List<Consumer<SearchLogContext>> start(Task task, SearchRequest request) {
        List<Consumer<SearchLogContext>> followers = null;
        for (ActivityLogListener<SearchLogContext, SearchRequest> listener : listeners) {
            Consumer<SearchLogContext> follower;
            try {
                follower = listener.start(task, request);
            } catch (Exception e) {
                // a listener must never fail the search
                logger.warn(() -> "a search log listener [" + listener + "] failed", e);
                continue;
            }
            if (follower != null) {
                if (followers == null) {
                    followers = new ArrayList<>(1);
                }
                followers.add(follower);
            }
        }
        return followers == null ? List.of() : followers;
    }
}
