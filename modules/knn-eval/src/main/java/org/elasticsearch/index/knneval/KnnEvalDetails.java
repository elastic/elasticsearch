/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The opt-in per-query part of a response: copied ids and scores only, never pooled hits. */
public final class KnnEvalDetails {

    private KnnEvalDetails() {}

    /**
     * One query's result for one knob set, annotated so a caller can see <em>which</em> documents it got wrong without joining two
     * lists by hand. {@code relevantRetrieved} equals the number of hits with a non-null {@code baseline_rank}, and
     * {@code relevant - relevantRetrieved} equals {@code missed.size()}.
     *
     * @param epsilonProfile per-rank fidelity loss, {@code null} where unbounded or beyond the baseline's depth; empty when fidelity
     *                       was skipped
     * @param incomplete     true when some rank had an unbounded loss, which is why the query is counted in
     *                       {@code fidelity.infinite_count} instead of {@code fidelity.max_epsilon}
     */
    public record QueryDetail(
        double recall,
        long relevantRetrieved,
        long relevant,
        List<RankedHit> hits,
        List<RankedHit> missed,
        List<Double> epsilonProfile,
        boolean incomplete,
        @Nullable Double recallValue,
        long valueMatches
    ) implements Writeable, ToXContentObject {

        static final ParseField RECALL_FIELD = new ParseField("recall");
        static final ParseField RELEVANT_RETRIEVED_FIELD = new ParseField("relevant_retrieved");
        static final ParseField RELEVANT_FIELD = new ParseField("relevant");
        static final ParseField HITS_FIELD = new ParseField("hits");
        static final ParseField MISSED_FIELD = new ParseField("missed");
        static final ParseField RECALL_VALUE_FIELD = new ParseField("recall_value");
        static final ParseField VALUE_MATCHES_FIELD = new ParseField("value_matches");
        static final ParseField EPSILON_PROFILE_FIELD = new ParseField("epsilon_profile");
        static final ParseField INCOMPLETE_FIELD = new ParseField("incomplete");

        public QueryDetail(
            double recall,
            long relevantRetrieved,
            long relevant,
            List<RankedHit> hits,
            List<RankedHit> missed,
            List<Double> epsilonProfile,
            boolean incomplete,
            @Nullable Double recallValue,
            long valueMatches
        ) {
            this.recall = recall;
            this.relevantRetrieved = relevantRetrieved;
            this.relevant = relevant;
            this.hits = List.copyOf(hits);
            this.missed = List.copyOf(missed);
            // nulls are meaningful here, so this cannot be List#copyOf
            this.epsilonProfile = Collections.unmodifiableList(new ArrayList<>(epsilonProfile));
            this.incomplete = incomplete;
            this.recallValue = recallValue;
            this.valueMatches = valueMatches;
        }

        QueryDetail(StreamInput in) throws IOException {
            this(
                in.readDouble(),
                in.readVLong(),
                in.readVLong(),
                in.readCollectionAsList(RankedHit::new),
                in.readCollectionAsList(RankedHit::new),
                in.readCollectionAsList(StreamInput::readOptionalDouble),
                in.readBoolean(),
                in.readOptionalDouble(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(recall);
            out.writeVLong(relevantRetrieved);
            out.writeVLong(relevant);
            out.writeCollection(hits);
            out.writeCollection(missed);
            out.writeCollection(epsilonProfile, StreamOutput::writeOptionalDouble);
            out.writeBoolean(incomplete);
            out.writeOptionalDouble(recallValue);
            out.writeVLong(valueMatches);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RECALL_FIELD.getPreferredName(), recall);
            builder.field(RELEVANT_RETRIEVED_FIELD.getPreferredName(), relevantRetrieved);
            builder.field(RELEVANT_FIELD.getPreferredName(), relevant);
            builder.startArray(HITS_FIELD.getPreferredName());
            for (RankedHit hit : hits) {
                hit.toXContent(builder, params);
            }
            builder.endArray();
            builder.startArray(MISSED_FIELD.getPreferredName());
            for (RankedHit hit : missed) {
                hit.toXContent(builder, params);
            }
            builder.endArray();
            if (recallValue != null) {
                builder.field(RECALL_VALUE_FIELD.getPreferredName(), recallValue);
                builder.field(VALUE_MATCHES_FIELD.getPreferredName(), valueMatches);
            }
            if (epsilonProfile.isEmpty() == false) {
                builder.startArray(EPSILON_PROFILE_FIELD.getPreferredName());
                for (Double epsilon : epsilonProfile) {
                    builder.value(epsilon);
                }
                builder.endArray();
                builder.field(INCOMPLETE_FIELD.getPreferredName(), incomplete);
            }
            builder.endObject();
            return builder;
        }
    }

    /** One query's reference hit list, reported once rather than repeated under every knob set that is scored against it. */
    public record BaselineDetail(List<Hit> hits) implements Writeable, ToXContentObject {

        static final ParseField HITS_FIELD = new ParseField("hits");

        public BaselineDetail(List<Hit> hits) {
            this.hits = List.copyOf(hits);
        }

        BaselineDetail(StreamInput in) throws IOException {
            this(in.readCollectionAsList(Hit::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(hits);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(HITS_FIELD.getPreferredName());
            for (Hit hit : hits) {
                hit.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }

    /**
     * A document with the rank the baseline gave it, used for both a returned hit and a missed baseline hit.
     *
     * @param baselineRank {@code null} when the baseline never returned the document, which is this metric's false positive
     */
    public record RankedHit(String id, float score, @Nullable Integer baselineRank) implements Writeable, ToXContentObject {

        static final ParseField BASELINE_RANK_FIELD = new ParseField("baseline_rank");

        RankedHit(StreamInput in) throws IOException {
            this(in.readString(), in.readFloat(), in.readOptionalVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeFloat(score);
            out.writeOptionalVInt(baselineRank);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Hit.ID_FIELD.getPreferredName(), id);
            builder.field(Hit.SCORE_FIELD.getPreferredName(), score);
            if (baselineRank == null) {
                builder.nullField(BASELINE_RANK_FIELD.getPreferredName());
            } else {
                builder.field(BASELINE_RANK_FIELD.getPreferredName(), baselineRank);
            }
            builder.endObject();
            return builder;
        }
    }

    /** A returned document, reduced to what recall estimation needs. */
    public record Hit(String id, float score) implements Writeable, ToXContentObject {

        static final ParseField ID_FIELD = new ParseField("_id");
        static final ParseField SCORE_FIELD = new ParseField("_score");

        Hit(StreamInput in) throws IOException {
            this(in.readString(), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeFloat(score);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ID_FIELD.getPreferredName(), id);
            builder.field(SCORE_FIELD.getPreferredName(), score);
            builder.endObject();
            return builder;
        }
    }
}
