/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.StatsLevel;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.StatsLevel.genIllegalClusterLevelException;

public class SearchableSnapshotsStatsRequest extends BroadcastRequest<SearchableSnapshotsStatsRequest> {

    private StatsLevel level = StatsLevel.INDICES;

    SearchableSnapshotsStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SearchableSnapshotsStatsRequest(String... indices) {
        super(indices);
    }

    public SearchableSnapshotsStatsRequest(String[] indices, IndicesOptions indicesOptions) {
        super(indices, indicesOptions);
    }

    public void level(StatsLevel level) {
        this.level = Objects.requireNonNull(level, "level must not be null");
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (level == StatsLevel.NODE) {
            validationException = genIllegalClusterLevelException(level.name());
        }
        return validationException;
    }
}
