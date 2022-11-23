/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public record HealthIndicatorResult(
    String name,
    HealthStatus status,
    String symptom,
    HealthIndicatorDetails details,
    List<HealthIndicatorImpact> impacts,
    List<Diagnosis> diagnosisList
) implements ChunkedToXContent {
    @Override
    public Iterator<? extends ToXContent> toXContentChunked() {
        Iterator<? extends ToXContent> diagnosisIterator = Collections.emptyIterator();
        if (diagnosisList != null && diagnosisList.isEmpty() == false) {
            diagnosisIterator = diagnosisList.stream()
                .flatMap(s -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(s.toXContentChunked(), Spliterator.ORDERED), false))
                .iterator();
        }
        return Iterators.concat(Iterators.single((ToXContent) (builder, params) -> {
            builder.startObject();
            builder.field("status", status.xContentValue());
            builder.field("symptom", symptom);
            if (details != null && HealthIndicatorDetails.EMPTY.equals(details) == false) {
                builder.field("details", details, params);
            }
            if (impacts != null && impacts.isEmpty() == false) {
                builder.field("impacts", impacts);
            }
            if (diagnosisList != null && diagnosisList.isEmpty() == false) {
                builder.startArray("diagnosis");
            }
            return builder;
        }), diagnosisIterator, Iterators.single((builder, params) -> {
            if (diagnosisList != null && diagnosisList.isEmpty() == false) {
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }));
    }
}
