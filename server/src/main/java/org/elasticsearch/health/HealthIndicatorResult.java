/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.common.xcontent.NewChunkedXContentBuilder;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.collect.Iterators.flatMap;
import static org.elasticsearch.common.xcontent.NewChunkedXContentBuilder.array;
import static org.elasticsearch.common.xcontent.NewChunkedXContentBuilder.chunk;
import static org.elasticsearch.common.xcontent.NewChunkedXContentBuilder.empty;

public record HealthIndicatorResult(
    String name,
    HealthStatus status,
    String symptom,
    HealthIndicatorDetails details,
    List<HealthIndicatorImpact> impacts,
    List<Diagnosis> diagnosisList
) implements ChunkedToXContentObject {
    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return NewChunkedXContentBuilder.object(chunk((b, p) -> {
            b.field("status", status.xContentValue());
            b.field("symptom", symptom);
            if (details != null && HealthIndicatorDetails.EMPTY.equals(details) == false) {
                b.field("details", details, p);
            }
            if (impacts != null && impacts.isEmpty() == false) {
                b.field("impacts", impacts);
            }
            return b;
        }),
            diagnosisList != null && diagnosisList.isEmpty() == false
                ? array("diagnosis", flatMap(diagnosisList.iterator(), d -> d.toXContentChunked(params)))
                : empty()
        );
    }
}
