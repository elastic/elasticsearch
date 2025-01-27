/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public record HealthIndicatorResult(
    String name,
    HealthStatus status,
    String symptom,
    HealthIndicatorDetails details,
    List<HealthIndicatorImpact> impacts,
    List<Diagnosis> diagnosisList
) implements ChunkedToXContentObject {

    private Optional<Collection<Diagnosis>> diagnosis() {
        return diagnosisList != null && diagnosisList.isEmpty() == false ? Optional.of(diagnosisList) : Optional.empty();
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(ChunkedToXContentHelper.chunk((builder, params) -> {
            builder.startObject();
            builder.field("status", status.xContentValue());
            builder.field("symptom", symptom);
            if (details != null && HealthIndicatorDetails.EMPTY.equals(details) == false) {
                builder.field("details", details, params);
            }
            if (impacts != null && impacts.isEmpty() == false) {
                builder.field("impacts", impacts);
            }
            if (diagnosis().isPresent()) {
                // don't want to have a new chunk & nested iterator for this, so we start the object here
                builder.startObject("diagnosis");
            }
            return builder;
        }),
            diagnosis().map(d -> Iterators.flatMap(d.iterator(), s -> s.toXContentChunked(outerParams)))
                .orElse(Collections.emptyIterator()),
            ChunkedToXContentHelper.chunk((b, p) -> {
                if (diagnosis().isPresent()) {
                    b.endObject();
                }
                return b.endObject();
            })
        );
    }
}
