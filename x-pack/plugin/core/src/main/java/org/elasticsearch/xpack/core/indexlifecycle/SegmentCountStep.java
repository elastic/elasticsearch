/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
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
            long numberShardsLeftToMerge = StreamSupport.stream(response.getIndices().get(index.getName()).spliterator(), false)
                    .filter(iss -> Arrays.stream(iss.getShards()).anyMatch(p -> {
                    boolean hasRightAmountOfSegments = p.getSegments().size() <= maxNumSegments;
                    if (bestCompression) {
//                        // TODO(talevy): discuss
//                        boolean allUsingCorrectCompression = p.getSegments().stream().anyMatch(s ->
//                            Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION.equals(
//                                Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION.toString().equals(
//                                    s.getAttributes().get(Lucene50StoredFieldsFormat.MODE_KEY)))
//                        );
                        boolean allUsingCorrectCompression = true;
                            return (hasRightAmountOfSegments && allUsingCorrectCompression) == false;
                    } else {
                            return hasRightAmountOfSegments == false;
                    }
                    })).count();
            listener.onResponse(numberShardsLeftToMerge == 0, new Info(numberShardsLeftToMerge));
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

    public static class Info implements ToXContentObject {

        private final long numberShardsLeftToMerge;

        static final ParseField SHARDS_TO_MERGE = new ParseField("shards_left_to_merge");
        static final ParseField MESSAGE = new ParseField("message");
        static final ConstructingObjectParser<Info, Void> PARSER = new ConstructingObjectParser<>("segment_count_step_info",
                a -> new Info((long) a[0]));
        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SHARDS_TO_MERGE);
            PARSER.declareString((i, s) -> {}, MESSAGE);
        }

        public Info(long numberShardsLeftToMerge) {
            this.numberShardsLeftToMerge = numberShardsLeftToMerge;
        }

        public long getNumberShardsLeftToMerge() {
            return numberShardsLeftToMerge;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(),
                    "Waiting for [" + numberShardsLeftToMerge + "] shards " + "to forcemerge");
            builder.field(SHARDS_TO_MERGE.getPreferredName(), numberShardsLeftToMerge);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(numberShardsLeftToMerge);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Info other = (Info) obj;
            return Objects.equals(numberShardsLeftToMerge, other.numberShardsLeftToMerge);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
