/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.eql;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.InstantiatingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class EqlSearchResponse {

    private final Hits hits;
    private final long tookInMillis;
    private final boolean isTimeout;
    private final String asyncExecutionId;
    private final boolean isRunning;
    private final boolean isPartial;

    private static final class Fields {
        static final String TOOK = "took";
        static final String TIMED_OUT = "timed_out";
        static final String HITS = "hits";
        static final String ID = "id";
        static final String IS_RUNNING = "is_running";
        static final String IS_PARTIAL = "is_partial";
    }

    private static final ParseField TOOK = new ParseField(Fields.TOOK);
    private static final ParseField TIMED_OUT = new ParseField(Fields.TIMED_OUT);
    private static final ParseField HITS = new ParseField(Fields.HITS);
    private static final ParseField ID = new ParseField(Fields.ID);
    private static final ParseField IS_RUNNING = new ParseField(Fields.IS_RUNNING);
    private static final ParseField IS_PARTIAL = new ParseField(Fields.IS_PARTIAL);

    private static final InstantiatingObjectParser<EqlSearchResponse, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<EqlSearchResponse, Void> parser =
            InstantiatingObjectParser.builder("eql/search_response", true, EqlSearchResponse.class);
        parser.declareObject(constructorArg(), (p, c) -> Hits.fromXContent(p), HITS);
        parser.declareLong(constructorArg(), TOOK);
        parser.declareBoolean(constructorArg(), TIMED_OUT);
        parser.declareString(optionalConstructorArg(), ID);
        parser.declareBoolean(constructorArg(), IS_RUNNING);
        parser.declareBoolean(constructorArg(), IS_PARTIAL);
        PARSER = parser.build();
    }

    public EqlSearchResponse(Hits hits, long tookInMillis, boolean isTimeout, String asyncExecutionId,
                             boolean isRunning, boolean isPartial) {
        super();
        this.hits = hits == null ? Hits.EMPTY : hits;
        this.tookInMillis = tookInMillis;
        this.isTimeout = isTimeout;
        this.asyncExecutionId = asyncExecutionId;
        this.isRunning = isRunning;
        this.isPartial = isPartial;
    }

    public static EqlSearchResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
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

    public String id() {
        return asyncExecutionId;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public boolean isPartial() {
        return isPartial;
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

    // Sequence
    public static class Sequence {
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

        public static Sequence fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
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

        public List<String> joinKeys() {
            return joinKeys;
        }

        public List<SearchHit> events() {
            return events;
        }
    }

    // Count
    public static class Count {
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

        public static Count fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
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

        public int count() {
            return count;
        }

        public List<String> keys() {
            return keys;
        }

        public float percent() {
            return percent;
        }
    }

    // Hits
    public static class Hits {
        public static final Hits EMPTY = new Hits(null, null, null, null);

        private final List<SearchHit> events;
        private final List<Sequence> sequences;
        private final List<Count> counts;
        private final TotalHits totalHits;

        private static final class Fields {
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

        public List<SearchHit> events() {
            return this.events;
        }

        public List<Sequence> sequences() {
            return this.sequences;
        }

        public List<Count> counts() {
            return this.counts;
        }

        public TotalHits totalHits() {
            return this.totalHits;
        }
    }
}
