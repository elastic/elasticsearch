/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


/**
 * Response to perform an eql search
 *
 * Example events response:
 *         List&lt;SearchHit&gt; events = Arrays.asList(
 *             new SearchHit(1, "111", null),
 *             new SearchHit(2, "222", null)
 *         );
 *         EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(Arrays.asList(
 *             new EqlSearchResponse.Sequence(Collections.singletonList("4021"), events),
 *             new EqlSearchResponse.Sequence(Collections.singletonList("2343"), events)
 *         ), null, null, new TotalHits(0, TotalHits.Relation.EQUAL_TO));
 *         EqlSearchResponse response = new EqlSearchResponse(hits, 5, false);
 *
 *
 *  Example sequence response:
 *         List&lt;SearchHit&gt; events1 = Arrays.asList(
 *             new SearchHit(1, "111", null),
 *             new SearchHit(2, "222", null)
 *         );
 *         List&lt;SearchHit&gt; events2 = Arrays.asList(
 *             new SearchHit(1, "333", null),
 *             new SearchHit(2, "444", null)
 *         );
 *         List&lt;Sequence&gt; sequences = Arrays.asList(
 *                 new EqlSearchResponse.Sequence(new String[]{"4021"}, events1),
 *                 new EqlSearchResponse.Sequence(new String[]{"2343"}, events2)
 *             );
 *
 *         EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(null, sequences, null, new TotalHits(100, TotalHits.Relation.EQUAL_TO));
 *         EqlSearchResponse response = new EqlSearchResponse(hits, 5, false);
 *
 *
 *  Example count response:
 *         TotalHits totals = new TotalHits(100, TotalHits.Relation.EQUAL_TO);
 *         List&lt;Count&gt; counts = Arrays.asList(
 *                 new EqlSearchResponse.Count(40, new String[]{"foo", "bar"}, .42233f),
 *                 new EqlSearchResponse.Count(15, new String[]{"foo", "bar"}, .170275f)
 *         );
 *
 *         EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(null, null, counts, totals);
 *         EqlSearchResponse response = new EqlSearchResponse(hits, 5, false);
 */
public class EqlSearchResponse extends ActionResponse implements ToXContentObject {

    private final Hits hits;
    private final long tookInMillis;
    private final boolean isTimeout;

    private static final class Fields {
        static final String TOOK = "took";
        static final String TIMED_OUT = "timed_out";
        static final String HITS = "hits";
    }

    private static final ParseField TOOK = new ParseField(Fields.TOOK);
    private static final ParseField TIMED_OUT = new ParseField(Fields.TIMED_OUT);
    private static final ParseField HITS = new ParseField(Fields.HITS);

    private static final ConstructingObjectParser<EqlSearchResponse, Void> PARSER =
        new ConstructingObjectParser<>("eql/search_response", true,
            args -> {
                int i = 0;
                Hits hits = (Hits) args[i++];
                Long took = (Long) args[i++];
                Boolean timeout = (Boolean) args[i];
                return new EqlSearchResponse(hits, took, timeout);
            });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Hits.fromXContent(p), HITS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOOK);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), TIMED_OUT);
    }

    public EqlSearchResponse(Hits hits, long tookInMillis, boolean isTimeout) {
        super();
        this.hits = hits == null ? Hits.EMPTY : hits;
        this.tookInMillis = tookInMillis;
        this.isTimeout = isTimeout;
    }

    public EqlSearchResponse(StreamInput in) throws IOException {
        super(in);
        tookInMillis = in.readVLong();
        isTimeout = in.readBoolean();
        hits = new Hits(in);
    }

    public static EqlSearchResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(tookInMillis);
        out.writeBoolean(isTimeout);
        hits.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    private XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TOOK.getPreferredName(), tookInMillis);
        builder.field(TIMED_OUT.getPreferredName(), isTimeout);
        hits.toXContent(builder, params);
        return builder;
    }

    public long took() {
        return tookInMillis;
    }

    public boolean isTimeout() {
        return isTimeout;
    }

    public Hits hits() {
        return hits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EqlSearchResponse that = (EqlSearchResponse) o;
        return Objects.equals(hits, that.hits)
            && Objects.equals(tookInMillis, that.tookInMillis)
            && Objects.equals(isTimeout, that.isTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits, tookInMillis, isTimeout);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }


    // Sequence
    public static class Sequence implements Writeable, ToXContentObject {
        private static final class Fields {
            static final String JOIN_KEYS = "join_keys";
            static final String EVENTS = "events";
        }

        private static final ParseField JOIN_KEYS = new ParseField(Fields.JOIN_KEYS);
        private static final ParseField EVENTS = new ParseField(Fields.EVENTS);

        private static final ConstructingObjectParser<EqlSearchResponse.Sequence, Void> PARSER =
            new ConstructingObjectParser<>("eql/search_response_sequence", true,
                args -> {
                    int i = 0;
                    @SuppressWarnings("unchecked") List<String> joinKeys = (List<String>) args[i++];
                    @SuppressWarnings("unchecked") List<SearchHit> events = (List<SearchHit>) args[i];
                    return new EqlSearchResponse.Sequence(joinKeys, events);
                });

        static {
            PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), JOIN_KEYS);
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> SearchHit.fromXContent(p), EVENTS);
        }

        private final List<String> joinKeys;
        private final List<SearchHit> events;

        public Sequence(List<String> joinKeys, List<SearchHit> events) {
            this.joinKeys = joinKeys == null ? Collections.emptyList() : joinKeys;
            this.events = events == null ? Collections.emptyList() : events;
        }

        public Sequence(StreamInput in) throws IOException {
            this.joinKeys = in.readStringList();
            this.events = in.readList(SearchHit::new);
        }

        public static Sequence fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(joinKeys);
            out.writeList(events);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (joinKeys.isEmpty() == false) {
                builder.field(Fields.JOIN_KEYS, joinKeys);
            }
            if (events.isEmpty() == false) {
                builder.startArray(EVENTS.getPreferredName());
                for (SearchHit event : events) {
                    event.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Sequence that = (Sequence) o;
            return Objects.equals(joinKeys, that.joinKeys)
                && Objects.equals(events, that.events);
        }

        @Override
        public int hashCode() {
            return Objects.hash(joinKeys, events);
        }
    }

    // Count
    public static class Count implements ToXContentObject, Writeable {
        private static final class Fields {
            static final String COUNT = "_count";
            static final String KEYS = "_keys";
            static final String PERCENT = "_percent";
        }

        private final int count;
        private final List<String> keys;
        private final float percent;

        private static final ParseField COUNT = new ParseField(Fields.COUNT);
        private static final ParseField KEYS = new ParseField(Fields.KEYS);
        private static final ParseField PERCENT = new ParseField(Fields.PERCENT);

        private static final ConstructingObjectParser<EqlSearchResponse.Count, Void> PARSER =
            new ConstructingObjectParser<>("eql/search_response_count", true,
                args -> {
                    int i = 0;
                    int count = (int) args[i++];
                    @SuppressWarnings("unchecked") List<String> joinKeys = (List<String>) args[i++];
                    float percent = (float) args[i];
                    return new EqlSearchResponse.Count(count, joinKeys, percent);
                });

        static {
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), COUNT);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), KEYS);
            PARSER.declareFloat(ConstructingObjectParser.constructorArg(), PERCENT);
        }

        public Count(int count, List<String> keys, float percent) {
            this.count = count;
            this.keys = keys == null ? Collections.emptyList() : keys;
            this.percent = percent;
        }

        public Count(StreamInput in) throws IOException {
            count = in.readVInt();
            keys = in.readStringList();
            percent = in.readFloat();
        }

        public static Count fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
            out.writeStringCollection(keys);
            out.writeFloat(percent);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.COUNT, count);
            builder.field(Fields.KEYS, keys);
            builder.field(Fields.PERCENT, percent);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Count that = (Count) o;
            return Objects.equals(count, that.count)
                && Objects.equals(keys, that.keys)
                && Objects.equals(percent, that.percent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, keys, percent);
        }
    }

    // Hits
    public static class Hits implements Writeable, ToXContentFragment {
        public static final Hits EMPTY = new Hits(null, null, null, null);

        private final List<SearchHit> events;
        private final List<Sequence> sequences;
        private final List<Count> counts;
        private final TotalHits totalHits;

        private static final class Fields {
            static final String HITS = "hits";
            static final String TOTAL = "total";
            static final String EVENTS = "events";
            static final String SEQUENCES = "sequences";
            static final String COUNTS = "counts";
        }

        public Hits(@Nullable List<SearchHit> events, @Nullable List<Sequence> sequences, @Nullable List<Count> counts,
                    @Nullable TotalHits totalHits) {
            this.events = events;
            this.sequences = sequences;
            this.counts = counts;
            this.totalHits = totalHits;
        }


        public Hits(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                totalHits = Lucene.readTotalHits(in);
            } else {
                totalHits = null;
            }
            events =  in.readBoolean() ? in.readList(SearchHit::new) : null;
            sequences = in.readBoolean() ? in.readList(Sequence::new) : null;
            counts = in.readBoolean() ? in.readList(Count::new) : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final boolean hasTotalHits = totalHits != null;
            out.writeBoolean(hasTotalHits);
            if (hasTotalHits) {
                Lucene.writeTotalHits(out, totalHits);
            }
            if (events != null) {
                out.writeBoolean(true);
                out.writeList(events);
            } else {
                out.writeBoolean(false);
            }
            if (sequences != null) {
                out.writeBoolean(true);
                out.writeList(sequences);
            } else {
                out.writeBoolean(false);
            }
            if (counts != null) {
                out.writeBoolean(true);
                out.writeList(counts);
            } else {
                out.writeBoolean(false);
            }
        }

        private static final ConstructingObjectParser<EqlSearchResponse.Hits, Void> PARSER =
            new ConstructingObjectParser<>("eql/search_response_count", true,
                args -> {
                    int i = 0;
                    @SuppressWarnings("unchecked") List<SearchHit> searchHits = (List<SearchHit>) args[i++];
                    @SuppressWarnings("unchecked") List<Sequence> sequences = (List<Sequence>) args[i++];
                    @SuppressWarnings("unchecked") List<Count> counts = (List<Count>) args[i++];
                    TotalHits totalHits = (TotalHits) args[i];
                    return new EqlSearchResponse.Hits(searchHits, sequences, counts, totalHits);
                });

        static {
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> SearchHit.fromXContent(p),
                new ParseField(Fields.EVENTS));
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), Sequence.PARSER,
                new ParseField(Fields.SEQUENCES));
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), Count.PARSER,
                new ParseField(Fields.COUNTS));
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> SearchHits.parseTotalHitsFragment(p),
                new ParseField(Fields.TOTAL));
        }

        public static Hits fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.HITS);
            if (totalHits != null) {
                builder.startObject(Fields.TOTAL);
                builder.field("value", totalHits.value);
                builder.field("relation", totalHits.relation == TotalHits.Relation.EQUAL_TO ? "eq" : "gte");
                builder.endObject();
            }
            if (events != null) {
                builder.startArray(Fields.EVENTS);
                for (SearchHit event : events) {
                    event.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (sequences != null) {
                builder.field(Fields.SEQUENCES, sequences);
            }
            if (counts != null) {
                builder.field(Fields.COUNTS, counts);
            }
            builder.endObject();

            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Hits that = (Hits) o;
            return Objects.equals(events, that.events)
                && Objects.equals(sequences, that.sequences)
                && Objects.equals(counts, that.counts)
                && Objects.equals(totalHits, that.totalHits);
        }

        @Override
        public int hashCode() {
            return Objects.hash(events, sequences, counts, totalHits);
        }
    }
}
