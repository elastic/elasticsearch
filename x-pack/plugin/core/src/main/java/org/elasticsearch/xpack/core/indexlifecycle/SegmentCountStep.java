/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * This {@link Step} evaluates whether force_merge was successful by checking the segment count.
 */
public class SegmentCountStep extends AsyncWaitStep {
    public static final String NAME = "segment-count";

    private final int maxNumSegments;

    public SegmentCountStep(StepKey key, StepKey nextStepKey, Client client, int maxNumSegments) {
        super(key, nextStepKey, client);
        this.maxNumSegments = maxNumSegments;
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener) {
        getClient().admin().indices().segments(new IndicesSegmentsRequest(indexMetaData.getIndex().getName()),
            ActionListener.wrap(response -> {
                long numberShardsLeftToMerge =
                    StreamSupport.stream(response.getIndices().get(indexMetaData.getIndex().getName()).spliterator(), false)
                        .filter(iss -> Arrays.stream(iss.getShards()).anyMatch(p -> p.getSegments().size() > maxNumSegments)).count();
                listener.onResponse(numberShardsLeftToMerge == 0, new Info(numberShardsLeftToMerge));
            }, listener::onFailure));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxNumSegments);
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
            && Objects.equals(maxNumSegments, other.maxNumSegments);
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
