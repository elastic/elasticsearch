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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.ObjectParser.fromList;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;


/**
 * Response to perform an eql search
 *
 * Example events response:
 *         EqlSearchResponse.Events events = new EqlSearchResponse.Events(new SearchHit[]{
 *             new SearchHit(1, "111", null),
 *             new SearchHit(2, "222", null),
 *         });
 *
 *         EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(events, new TotalHits(100, TotalHits.Relation.EQUAL_TO));
 *         EqlSearchResponse response = new EqlSearchResponse(hits, 5, false);
 *
 *
 *  Example sequence response:
 *         EqlSearchResponse.Events events1 = new EqlSearchResponse.Events(new SearchHit[]{
 *             new SearchHit(1, "111", null),
 *             new SearchHit(2, "222", null),
 *         });
 *         EqlSearchResponse.Events events2 = new EqlSearchResponse.Events(new SearchHit[]{
 *             new SearchHit(3, "333", null),
 *             new SearchHit(4, "444", null),
 *         });
 *         EqlSearchResponse.Sequences sequences = new EqlSearchResponse.Sequences(
 *             new EqlSearchResponse.Sequence[]{
 *                 new EqlSearchResponse.Sequence(new String[]{"4021"}, events1),
 *                 new EqlSearchResponse.Sequence(new String[]{"2343"}, events2)
 *             });
 *
 *         EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(sequences, new TotalHits(100, TotalHits.Relation.EQUAL_TO));
 *         EqlSearchResponse response = new EqlSearchResponse(hits, 5, false);
 *
 *
 *  Example count response:
 *         TotalHits totals = new TotalHits(100, TotalHits.Relation.EQUAL_TO);
 *         EqlSearchResponse.Counts counts = new EqlSearchResponse.Counts(
 *             new EqlSearchResponse.Count[]{
 *                 new EqlSearchResponse.Count(40, new String[]{"foo", "bar"}, .42233f),
 *                 new EqlSearchResponse.Count(15, new String[]{"foo", "bar"}, .170275f),
 *             }
 *         );
 *
 *         EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(counts, totals);
 *         EqlSearchResponse response = new EqlSearchResponse(hits, 5, false);
 */
public class EqlSearchResponse extends ActionResponse implements ToXContentObject {

    private Hits hits;
    private long tookInMillis;
    private boolean isTimeout;

    private static final class Fields {
        static final String TOOK = "took";
        static final String TIMED_OUT = "timed_out";
        static final String HITS = "hits";
    }

    private static final ParseField TOOK = new ParseField(Fields.TOOK);
    private static final ParseField TIMED_OUT = new ParseField(Fields.TIMED_OUT);
    private static final ParseField HITS = new ParseField(Fields.HITS);

    private static final ObjectParser<EqlSearchResponse, Void> PARSER = objectParser(EqlSearchResponse::new);

    private static <R extends EqlSearchResponse> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        ObjectParser<R, Void> parser = new ObjectParser<>("eql/search_response", false, supplier);
        parser.declareLong(EqlSearchResponse::took, TOOK);
        parser.declareBoolean(EqlSearchResponse::isTimeout, TIMED_OUT);
        parser.declareObject(EqlSearchResponse::hits,
            (p, c) -> Hits.fromXContent(p), HITS);
        return parser;
    }

    // Constructor for parser from json
    protected EqlSearchResponse() {
        super();
    }

    public EqlSearchResponse(Hits hits, long tookInMillis, boolean isTimeout) {
        super();
        this.hits(hits);
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

    public void took(long tookInMillis) {
        this.tookInMillis = tookInMillis;
    }

    public boolean isTimeout() {
        return isTimeout;
    }

    public void isTimeout(boolean isTimeout) {
        this.isTimeout = isTimeout;
    }

    public Hits hits() {
        return hits;
    }

    public void hits(Hits hits) {
        if (hits == null) {
            this.hits = new Hits((Events)null, null);
        } else {
            this.hits = hits;
        }
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
        }

        private static final ParseField JOIN_KEYS = new ParseField(Fields.JOIN_KEYS);
        private static final ParseField EVENTS = new ParseField(Events.NAME);

        private static final ObjectParser<EqlSearchResponse.Sequence, Void> PARSER = objectParser(EqlSearchResponse.Sequence::new);

        private static <R extends EqlSearchResponse.Sequence> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
            ObjectParser<R, Void> parser = new ObjectParser<>("eql/search_response_sequence", false, supplier);
            parser.declareStringArray(fromList(String.class, EqlSearchResponse.Sequence::joinKeys), JOIN_KEYS);
            parser.declareObjectArray(Sequence::setEvents,
                (p, c) -> SearchHit.fromXContent(p), EVENTS);
            return parser;
        }

        private String[] joinKeys = null;
        private Events events = null;

        private Sequence(){
            this(null, null);
        }

        public Sequence(String[] joinKeys, Events events) {
            this.joinKeys(joinKeys);
            if (events == null) {
                this.events = new Events((SearchHit[])(null));
            } else {
                this.events = events;
            }
        }

        public Sequence(StreamInput in) throws IOException {
            this.joinKeys = in.readStringArray();
            this.events = new Events(in);
        }

        public void joinKeys(String[] joinKeys) {
            if (joinKeys == null) {
                this.joinKeys = new String[0];
            } else {
                this.joinKeys = joinKeys;
            }
        }

        private void setEvents(List<SearchHit> hits) {
            if (hits == null) {
                this.events = new Events((SearchHit[])(null));
            } else {
                this.events = new Events(hits.toArray(new SearchHit[hits.size()]));
            }
        }

        public static Sequence fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(joinKeys);
            out.writeVInt(events.entries().length);
            if (events.entries().length > 0) {
                for (SearchHit hit : events.entries()) {
                    hit.writeTo(out);
                }
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (joinKeys != null) {
                builder.startArray(Fields.JOIN_KEYS);
                for(String s : joinKeys) {
                    builder.value(s);
                }
                builder.endArray();
            }
            if (events != null) {
                events.toXContent(builder, params);
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
            return Arrays.equals(joinKeys, that.joinKeys)
                && Objects.equals(events, that.events);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(joinKeys), events);
        }
    }

    // Count
    public static class Count implements ToXContentObject, Writeable {
        private static final class Fields {
            static final String COUNT = "_count";
            static final String KEYS = "_keys";
            static final String PERCENT = "_percent";
        }

        private int count;
        private String[] keys;
        private float percent;

        private static final ParseField COUNT = new ParseField(Fields.COUNT);
        private static final ParseField KEYS = new ParseField(Fields.KEYS);
        private static final ParseField PERCENT = new ParseField(Fields.PERCENT);

        private static final ObjectParser<EqlSearchResponse.Count, Void> PARSER = objectParser(EqlSearchResponse.Count::new);

        protected static <R extends EqlSearchResponse.Count> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
            ObjectParser<R, Void> parser = new ObjectParser<>("eql/search_response_count", false, supplier);
            parser.declareInt(EqlSearchResponse.Count::count, COUNT);
            parser.declareStringArray(fromList(String.class, EqlSearchResponse.Count::keys), KEYS);
            parser.declareFloat(EqlSearchResponse.Count::percent, PERCENT);
            return parser;
        }

        private Count() {}

        public Count(int count, String[] keys, float percent) {
            this.count = count;
            this.keys(keys);
            this.percent = percent;
        }

        public Count(StreamInput in) throws IOException {
            count = in.readVInt();
            keys = in.readStringArray();
            percent = in.readFloat();
        }

        public void count(int count) {
            this.count = count;
        }

        public void keys(String[] keys) {
            if (keys == null) {
                this.keys = new String[0];
            } else {
                this.keys = keys;
            }
        }

        public void percent(float percent) {
            this.percent = percent;
        }

        public static Count fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
            out.writeStringArray(keys);
            out.writeFloat(percent);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.COUNT, count);
            builder.array(Fields.KEYS, keys);
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
                && Arrays.equals(keys, that.keys)
                && Objects.equals(percent, that.percent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, Arrays.hashCode(keys), percent);
        }
    }

    // Base class for serialized collection of entries (events, sequences, counts)
    private abstract static class Entries<T extends Writeable & ToXContentObject> implements Writeable, ToXContentFragment {
        private final String name;
        private final T[] entries;

        Entries(String name, T[] entries) {
            this.name = name;
            if (entries == null) {
                this.entries =  createEntriesArray(0);
            } else {
                this.entries = entries;
            }
        }

        Entries(String name, StreamInput in) throws IOException {
            this.name = name;
            int size = in.readVInt();
            entries = createEntriesArray(size);
            for (int i = 0; i < size; i++) {
                entries[i] = createEntry(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(entries.length);
            if (entries.length > 0) {
                for (T e : entries) {
                    e.writeTo(out);
                }
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(name);
            if (entries != null) {
                for (T e : entries) {
                    e.toXContent(builder, params);
                }
            }
            builder.endArray();
            return null;
        }

        public T[] entries() {
            return entries;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entries<?> that = (Entries<?>) o;
            return Arrays.deepEquals(entries, that.entries);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(entries);
        }

        protected abstract T[] createEntriesArray(int size);
        protected abstract T createEntry(StreamInput in) throws IOException;
    }

    // Events
    public static class Events extends EqlSearchResponse.Entries<SearchHit> {
        private static final String NAME = "events";

        public Events(SearchHit[] entries) {
            super(NAME, entries);
        }

        public Events(StreamInput in) throws IOException {
            super(NAME, in);
        }

        @Override
        protected final SearchHit[] createEntriesArray(int size) {
            return new SearchHit[size];
        }

        @Override
        protected final SearchHit createEntry(StreamInput in) throws IOException {
            return new SearchHit(in);
        }
    }

    // Sequences
    public static class Sequences extends EqlSearchResponse.Entries<EqlSearchResponse.Sequence> {
        private static String NAME = "sequences";

        public Sequences(Sequence[] entries) {
            super(NAME, entries);
        }

        public Sequences(StreamInput in) throws IOException {
            super(NAME, in);
        }

        @Override
        protected final Sequence[] createEntriesArray(int size) {
            return new Sequence[size];
        }

        @Override
        protected final Sequence createEntry(StreamInput in) throws IOException {
            return new Sequence(in);
        }
    }

    // Counts
    public static class Counts extends EqlSearchResponse.Entries<EqlSearchResponse.Count> {
        private static String NAME = "counts";
        public Counts(Count[] entries) {
            super(NAME, entries);
        }

        public Counts(StreamInput in) throws IOException {
            super(NAME, in);
        }

        @Override
        protected final Count[] createEntriesArray(int size) {
            return new Count[size];
        }

        @Override
        protected final Count createEntry(StreamInput in) throws IOException {
            return new Count(in);
        }
    }

    // Hits
    public static class Hits implements Writeable, ToXContentFragment {
        private Events events = null;
        private Sequences sequences = null;
        private Counts counts = null;
        private TotalHits totalHits;

        private static final class Fields {
            static final String HITS = "hits";
            static final String TOTAL = "total";
        }

        public Hits(Events events, @Nullable TotalHits totalHits) {
            this.events = events;
            this.totalHits = totalHits;
        }

        public Hits(Sequences sequences, @Nullable TotalHits totalHits) {
            this.sequences = sequences;
            this.totalHits = totalHits;
        }

        public Hits(Counts counts, @Nullable TotalHits totalHits) {
            this.counts = counts;
            this.totalHits = totalHits;
        }

        public Hits(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                totalHits = Lucene.readTotalHits(in);
            } else {
                totalHits = null;
            }

            events = in.readBoolean() ? new Events(in) : null;
            sequences = in.readBoolean() ? new Sequences(in) : null;
            counts = in.readBoolean() ? new Counts(in) : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final boolean hasTotalHits = totalHits != null;
            out.writeBoolean(hasTotalHits);
            if (hasTotalHits) {
                Lucene.writeTotalHits(out, totalHits);
            }
            out.writeOptionalWriteable(events);
            out.writeOptionalWriteable(sequences);
            out.writeOptionalWriteable(counts);
        }

        public static Hits fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            }
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = null;
            TotalHits totalHits = null;
            ArrayList<SearchHit> searchHits = null;
            ArrayList<Sequence> sequences = null;
            ArrayList<Count> counts = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (Fields.TOTAL.equals(currentFieldName)) {
                        totalHits = new TotalHits(parser.longValue(), TotalHits.Relation.EQUAL_TO);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (Events.NAME.equals(currentFieldName)) {
                        searchHits = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            searchHits.add(SearchHit.fromXContent(parser));
                        }
                    } else if (Sequences.NAME.equals(currentFieldName)) {
                        sequences = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            sequences.add(Sequence.fromXContent(parser));
                        }
                    } else if (Counts.NAME.equals(currentFieldName)) {
                        counts = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            counts.add(Count.fromXContent(parser));
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (SearchHits.Fields.TOTAL.equals(currentFieldName)) {
                        totalHits = SearchHits.parseTotalHitsFragment(parser);
                    } else {
                        parser.skipChildren();
                    }
                }
            }

            if (searchHits != null) {
                return new EqlSearchResponse.Hits(new EqlSearchResponse.Events(searchHits.toArray(new SearchHit[searchHits.size()])),
                    totalHits);
            } else if (sequences != null) {
                return new EqlSearchResponse.Hits(new EqlSearchResponse.Sequences(sequences.toArray(new Sequence[sequences.size()])),
                    totalHits);
            } else if (counts != null) {
                return new EqlSearchResponse.Hits(new EqlSearchResponse.Counts(counts.toArray(new Count[counts.size()])), totalHits);
            }
            return new EqlSearchResponse.Hits((Events)null, totalHits);
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
                events.toXContent(builder, params);
            }
            if (sequences != null) {
                sequences.toXContent(builder, params);
            }
            if (counts != null) {
                counts.toXContent(builder, params);
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
