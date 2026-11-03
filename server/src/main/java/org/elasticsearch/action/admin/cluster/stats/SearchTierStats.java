/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Search tier information for inclusion in {@code GET _cluster/stats}, supplied by a {@link SearchTierStatsProvider}.
 * Emits the fields as a fragment; the caller is responsible for opening and closing the enclosing {@code search_tier} object.
 * New search tier fields should be added as additional record components.
 *
 * @param minSearchVcu the configured minimum search VCU floor, or {@code -1} if no floor is set
 */
public record SearchTierStats(int minSearchVcu) implements ToXContentFragment {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("min_search_vcu", minSearchVcu);
        return builder;
    }
}
