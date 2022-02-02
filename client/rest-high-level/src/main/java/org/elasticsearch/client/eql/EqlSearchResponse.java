/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.eql;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

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
        InstantiatingObjectParser.Builder<EqlSearchResponse, Void> parser = InstantiatingObjectParser.builder(
            "eql/search_response",
            true,
            EqlSearchResponse.class
        );
        parser.declareObject(constructorArg(), (p, c) -> Hits.fromXContent(p), HITS);
        parser.declareLong(constructorArg(), TOOK);
        parser.declareBoolean(constructorArg(), TIMED_OUT);
        parser.declareString(optionalConstructorArg(), ID);
        parser.declareBoolean(constructorArg(), IS_RUNNING);
        parser.declareBoolean(constructorArg(), IS_PARTIAL);
        PARSER = parser.build();
    }

    public EqlSearchResponse(
        Hits hits,
        long tookInMillis,
        boolean isTimeout,
        String asyncExecutionId,
        boolean isRunning,
        boolean isPartial
    ) {
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

    // Event
    public static class Event {

        private static final class Fields {
            static final String INDEX = GetResult._INDEX;
            static final String ID = GetResult._ID;
            static final String SOURCE = SourceFieldMapper.NAME;
        }

        private static final ParseField INDEX = new ParseField(Fields.INDEX);
        private static final ParseField ID = new ParseField(Fields.ID);
        private static final ParseField SOURCE = new ParseField(Fields.SOURCE);

        private static final ConstructingObjectParser<Event, Void> PARSER = new ConstructingObjectParser<>(
            "eql/search_response_event",
            true,
            args -> new Event((String) args[0], (String) args[1], (BytesReference) args[2])
        );

        static {
            PARSER.declareString(constructorArg(), INDEX);
            PARSER.declareString(constructorArg(), ID);
            PARSER.declareObject(constructorArg(), (p, c) -> {
                try (XContentBuilder builder = XContentBuilder.builder(p.contentType().xContent())) {
                    builder.copyCurrentStructure(p);
                    return BytesReference.bytes(builder);
                }
            }, SOURCE);
        }

        private final String index;
        private final String id;
        private final BytesReference source;
        private Map<String, Object> sourceAsMap;

        public Event(String index, String id, BytesReference source) {
            this.index = index;
            this.id = id;
            this.source = source;
        }

        public static Event fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        public String index() {
            return index;
        }

        public String id() {
            return id;
        }

        public BytesReference source() {
            return source;
        }

        public Map<String, Object> sourceAsMap() {
            if (source == null) {
                return null;
            }
            if (sourceAsMap != null) {
                return sourceAsMap;
            }

            sourceAsMap = SourceLookup.sourceAsMap(source);
            return sourceAsMap;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, id, source);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            EqlSearchResponse.Event other = (EqlSearchResponse.Event) obj;
            return Objects.equals(index, other.index) && Objects.equals(id, other.id) && Objects.equals(source, other.source);
        }
    }

    // Sequence
    public static class Sequence {
        private static final class Fields {
            static final String JOIN_KEYS = "join_keys";
            static final String EVENTS = "events";
        }

        private static final ParseField JOIN_KEYS = new ParseField(Fields.JOIN_KEYS);
        private static final ParseField EVENTS = new ParseField(Fields.EVENTS);

        private static final ConstructingObjectParser<EqlSearchResponse.Sequence, Void> PARSER = new ConstructingObjectParser<>(
            "eql/search_response_sequence",
            true,
            args -> {
                int i = 0;
                @SuppressWarnings("unchecked")
                List<Object> joinKeys = (List<Object>) args[i++];
                @SuppressWarnings("unchecked")
                List<Event> events = (List<Event>) args[i];
                return new EqlSearchResponse.Sequence(joinKeys, events);
            }
        );

        static {
            PARSER.declareFieldArray(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> XContentParserUtils.parseFieldsValue(p),
                JOIN_KEYS,
                ObjectParser.ValueType.VALUE_ARRAY
            );
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Event.fromXContent(p), EVENTS);
        }

        private final List<Object> joinKeys;
        private final List<Event> events;

        public Sequence(List<Object> joinKeys, List<Event> events) {
            this.joinKeys = joinKeys == null ? Collections.emptyList() : joinKeys;
            this.events = events == null ? Collections.emptyList() : events;
        }

        public static Sequence fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public List<Object> joinKeys() {
            return joinKeys;
        }

        public List<Event> events() {
            return events;
        }

        @Override
        public int hashCode() {
            return Objects.hash(joinKeys, events);
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
            return Objects.equals(joinKeys, that.joinKeys) && Objects.equals(events, that.events);
        }
    }

    // Hits
    public static class Hits {
        public static final Hits EMPTY = new Hits(null, null, null);

        private final List<Event> events;
        private final List<Sequence> sequences;
        private final TotalHits totalHits;

        private static final class Fields {
            static final String TOTAL = "total";
            static final String EVENTS = "events";
            static final String SEQUENCES = "sequences";
        }

        public Hits(@Nullable List<Event> events, @Nullable List<Sequence> sequences, @Nullable TotalHits totalHits) {
            this.events = events;
            this.sequences = sequences;
            this.totalHits = totalHits;
        }

        private static final ConstructingObjectParser<EqlSearchResponse.Hits, Void> PARSER = new ConstructingObjectParser<>(
            "eql/search_response_hits",
            true,
            args -> {
                int i = 0;
                @SuppressWarnings("unchecked")
                List<Event> events = (List<Event>) args[i++];
                @SuppressWarnings("unchecked")
                List<Sequence> sequences = (List<Sequence>) args[i++];
                TotalHits totalHits = (TotalHits) args[i];
                return new EqlSearchResponse.Hits(events, sequences, totalHits);
            }
        );

        static {
            PARSER.declareObjectArray(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> Event.fromXContent(p),
                new ParseField(Fields.EVENTS)
            );
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), Sequence.PARSER, new ParseField(Fields.SEQUENCES));
            PARSER.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> SearchHits.parseTotalHitsFragment(p),
                new ParseField(Fields.TOTAL)
            );
        }

        public static Hits fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public List<Event> events() {
            return this.events;
        }

        public List<Sequence> sequences() {
            return this.sequences;
        }

        public TotalHits totalHits() {
            return this.totalHits;
        }

        @Override
        public int hashCode() {
            return Objects.hash(events, sequences, totalHits);
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
                && Objects.equals(totalHits, that.totalHits);
        }
    }
}
