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
import java.util.List;

/**
 * A snapshot of the project's tag configuration for inclusion in {@code GET _cluster/stats}.
 * Populated by the serverless cross-project module (Ticket 4). Emits the static config fields
 * ({@code total}, {@code total_custom}, {@code names}, {@code named_routing_expressions}) inside
 * the top-level {@code tags} object.
 */
public record TagsConfigSnapshot(
    int total,
    int totalCustom,
    List<String> names,
    int namedRoutingExpressionsTotal,
    List<String> namedRoutingExpressionNames
) implements ToXContentFragment {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("total", total);
        builder.field("total_custom", totalCustom);
        builder.array("names", names.toArray(new String[0]));
        builder.startObject("named_routing_expressions");
        builder.field("total", namedRoutingExpressionsTotal);
        builder.array("names", namedRoutingExpressionNames.toArray(new String[0]));
        builder.endObject();
        return builder;
    }
}
