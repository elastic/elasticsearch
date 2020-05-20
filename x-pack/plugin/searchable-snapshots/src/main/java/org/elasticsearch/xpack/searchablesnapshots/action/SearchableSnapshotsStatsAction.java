/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionType;

public class SearchableSnapshotsStatsAction extends ActionType<SearchableSnapshotsStatsResponse> {

    public static final SearchableSnapshotsStatsAction INSTANCE = new SearchableSnapshotsStatsAction();
    static final String NAME = "cluster:monitor/xpack/searchable_snapshots/stats";

    private SearchableSnapshotsStatsAction() {
        super(NAME, SearchableSnapshotsStatsResponse::new);
    }
}
