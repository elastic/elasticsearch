/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * This {@link Step} evaluates whether force_merge was successful
 */
public class SegmentCountStep extends AsyncWaitStep {
    public static final String NAME = "segment-count";

    private final int maxNumSegments;
    private final boolean bestCompression;

    public SegmentCountStep(StepKey key, StepKey nextStepKey, Client client, int maxNumSegments, boolean bestCompression) {
        super(key, nextStepKey, client);
        this.maxNumSegments = maxNumSegments;
        this.bestCompression = bestCompression;
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    public boolean isBestCompression() {
        return bestCompression;
    }

    @Override
    public void evaluateCondition(Index index, Listener listener) {
        getClient().admin().indices().segments(new IndicesSegmentsRequest(index.getName()), ActionListener.wrap(response -> {
            listener.onResponse(StreamSupport.stream(response.getIndices().get(index.getName()).spliterator(), false)
                .anyMatch(iss -> Arrays.stream(iss.getShards()).anyMatch(p -> {
                    boolean hasRightAmountOfSegments = p.getSegments().size() <= maxNumSegments;
                    if (bestCompression) {
//                        // TODO(talevy): discuss
//                        boolean allUsingCorrectCompression = p.getSegments().stream().anyMatch(s ->
//                            Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION.equals(
//                                Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION.toString().equals(
//                                    s.getAttributes().get(Lucene50StoredFieldsFormat.MODE_KEY)))
//                        );
                        boolean allUsingCorrectCompression = true;
                        return hasRightAmountOfSegments && allUsingCorrectCompression;
                    } else {
                        return hasRightAmountOfSegments;
                    }
                })));
        }, listener::onFailure));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxNumSegments, bestCompression);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SegmentCountStep other = (SegmentCountStep) obj;
        return super.equals(obj)
            && Objects.equals(maxNumSegments, other.maxNumSegments)
            && Objects.equals(bestCompression, other.bestCompression);
    }
}
