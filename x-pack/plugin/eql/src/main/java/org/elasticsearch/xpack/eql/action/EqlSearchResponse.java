/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.ql.async.QlStatusResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.eql.util.SearchHitUtils.qualifiedIndex;

public class EqlSearchResponse extends ActionResponse implements ToXContentObject, QlStatusResponse.AsyncStatus {

    private final Hits hits;
    private final long tookInMillis;
    private final boolean isTimeout;
    private final String asyncExecutionId;
    private final boolean isRunning;
    private final boolean isPartial;
    private final ShardSearchFailure[] shardFailures;

    private static final class Fields {
        static final String TOOK = "took";
        static final String TIMED_OUT = "timed_out";
        static final String HITS = "hits";
        static final String ID = "id";
        static final String IS_RUNNING = "is_running";
        static final String IS_PARTIAL = "is_partial";
        static final String SHARD_FAILURES = "shard_failures";
    }

    private static final ParseField TOOK = new ParseField(Fields.TOOK);
    private static final ParseField TIMED_OUT = new ParseField(Fields.TIMED_OUT);
    private static final ParseField HITS = new ParseField(Fields.HITS);
    private static final ParseField ID = new ParseField(Fields.ID);
    private static final ParseField IS_RUNNING = new ParseField(Fields.IS_RUNNING);
    private static final ParseField IS_PARTIAL = new ParseField(Fields.IS_PARTIAL);
    private static final ParseField SHARD_FAILURES = new ParseField(Fields.SHARD_FAILURES);

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
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> ShardSearchFailure.EMPTY_ARRAY, SHARD_FAILURES);
        PARSER = parser.build();
    }

    public EqlSearchResponse(Hits hits, long tookInMillis, boolean isTimeout, ShardSearchFailure[] shardFailures) {
        this(hits, tookInMillis, isTimeout, null, false, false, shardFailures);
    }

    public EqlSearchResponse(
        Hits hits,
        long tookInMillis,
        boolean isTimeout,
        String asyncExecutionId,
        boolean isRunning,
        boolean isPartial,
        ShardSearchFailure[] shardFailures
    ) {
        super();
        this.hits = hits == null ? Hits.EMPTY : hits;
        this.tookInMillis = tookInMillis;
        this.isTimeout = isTimeout;
        this.asyncExecutionId = asyncExecutionId;
        this.isRunning = isRunning;
        this.isPartial = isPartial;
        this.shardFailures = shardFailures;
    }

    public EqlSearchResponse(StreamInput in) throws IOException {
        tookInMillis = in.readVLong();
        isTimeout = in.readBoolean();
        hits = new Hits(in);
        asyncExecutionId = in.readOptionalString();
        isPartial = in.readBoolean();
        isRunning = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersions.EQL_ALLOW_PARTIAL_SEARCH_RESULTS)) {
            shardFailures = in.readArray(ShardSearchFailure::readShardSearchFailure, ShardSearchFailure[]::new);
        } else {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        }
    }

    public static EqlSearchResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(tookInMillis);
        out.writeBoolean(isTimeout);
        hits.writeTo(out);
        out.writeOptionalString(asyncExecutionId);
        out.writeBoolean(isPartial);
        out.writeBoolean(isRunning);
        if (out.getTransportVersion().onOrAfter(TransportVersions.EQL_ALLOW_PARTIAL_SEARCH_RESULTS)) {
            out.writeArray(shardFailures);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    private XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (asyncExecutionId != null) {
            builder.field(ID.getPreferredName(), asyncExecutionId);
        }
        builder.field(IS_PARTIAL.getPreferredName(), isPartial);
        builder.field(IS_RUNNING.getPreferredName(), isRunning);
        builder.field(TOOK.getPreferredName(), tookInMillis);
        builder.field(TIMED_OUT.getPreferredName(), isTimeout);
        if (CollectionUtils.isEmpty(shardFailures) == false) {
            builder.startArray(SHARD_FAILURES.getPreferredName());
            for (ShardOperationFailedException shardFailure : ExceptionsHelper.groupBy(shardFailures)) {
                shardFailure.toXContent(builder, params);
            }
            builder.endArray();
        }
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
    public String id() {
        return asyncExecutionId;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public boolean isPartial() {
        return isPartial;
    }

    public ShardSearchFailure[] shardFailures() {
        return shardFailures;
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
            && Objects.equals(isTimeout, that.isTimeout)
            && Objects.equals(asyncExecutionId, that.asyncExecutionId)
            && Arrays.equals(shardFailures, that.shardFailures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits, tookInMillis, isTimeout, asyncExecutionId, Arrays.hashCode(shardFailures));
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    // Event
    public static class Event implements Writeable, ToXContentObject {

        public static final Event MISSING_EVENT = new Event("", "", new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), null, true);

        private static final class Fields {
            static final String INDEX = GetResult._INDEX;
            static final String ID = GetResult._ID;
            static final String SOURCE = SourceFieldMapper.NAME;
            static final String FIELDS = "fields";
            static final String MISSING = "missing";
        }

        private static final ParseField INDEX = new ParseField(Fields.INDEX);
        private static final ParseField ID = new ParseField(Fields.ID);
        private static final ParseField SOURCE = new ParseField(Fields.SOURCE);
        private static final ParseField FIELDS = new ParseField(Fields.FIELDS);
        private static final ParseField MISSING = new ParseField(Fields.MISSING);

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Event, Void> PARSER = new ConstructingObjectParser<>(
            "eql/search_response_event",
            true,
            args -> new Event(
                (String) args[0],
                (String) args[1],
                (BytesReference) args[2],
                (Map<String, DocumentField>) args[3],
                (Boolean) args[4]
            )
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
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
                Map<String, DocumentField> fields = new HashMap<>();
                while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                    DocumentField field = DocumentField.fromXContent(p);
                    fields.put(field.getName(), field);
                }
                return fields;
            }, FIELDS);
            PARSER.declareBoolean(optionalConstructorArg(), MISSING);
        }

        private String index;
        private final String id;
        private final BytesReference source;
        private final Map<String, DocumentField> fetchFields;

        private final boolean missing;

        public Event(SearchHit hit) {
            this(qualifiedIndex(hit), hit.getId(), hit.getSourceRef(), hit.getDocumentFields(), false);
        }

        public Event(String index, String id, BytesReference source, Map<String, DocumentField> fetchFields, Boolean missing) {
            this.index = index;
            this.id = id;
            this.source = source;
            this.fetchFields = fetchFields;
            this.missing = missing != null && missing;
        }

        private Event(StreamInput in) throws IOException {
            index = in.readString();
            id = in.readString();
            // TODO: make this pooled?
            source = in.readBytesReference();
            if (in.readBoolean()) {
                fetchFields = in.readMap(DocumentField::new);
            } else {
                fetchFields = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
                missing = in.readBoolean();
            } else {
                missing = index.isEmpty();
            }
        }

        public static Event readFrom(StreamInput in) throws IOException {
            Event result = new Event(in);
            return result.missing() ? MISSING_EVENT : result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(id);
            out.writeBytesReference(source);
            out.writeBoolean(fetchFields != null);
            if (fetchFields != null) {
                out.writeMap(fetchFields, StreamOutput::writeWriteable);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
                // for BWC, 8.9.1+ does not have "missing" attribute, but it considers events with an empty index "" as missing events
                // see https://github.com/elastic/elasticsearch/pull/98130
                out.writeBoolean(missing);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.INDEX, index);
            builder.field(Fields.ID, id);
            // We have to use the deprecated version since we don't know the content type of the original source
            XContentHelper.writeRawField(Fields.SOURCE, source, builder, params);
            // ignore fields all together if they are all empty
            if (fetchFields != null
                && fetchFields.isEmpty() == false
                && fetchFields.values().stream().anyMatch(df -> df.getValues().size() > 0)) {
                builder.startObject(Fields.FIELDS);
                for (DocumentField field : fetchFields.values()) {
                    if (field.getValues().size() > 0) {
                        field.getValidValuesWriter().toXContent(builder, params);
                    }
                }
                builder.endObject();
            }
            if (missing) {
                // preserve original event structure (before introduction of missing events): avoid "missing: false" for normal events
                builder.field(Fields.MISSING, missing);
            }
            builder.endObject();
            return builder;
        }

        public static Event fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        public void index(String index) {
            this.index = index;
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

        public Map<String, DocumentField> fetchFields() {
            return fetchFields;
        }

        public boolean missing() {
            return missing;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, id, source, fetchFields, missing);
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
            return Objects.equals(index, other.index)
                && Objects.equals(id, other.id)
                && Objects.equals(source, other.source)
                && Objects.equals(fetchFields, other.fetchFields)
                && Objects.equals(missing, other.missing);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }

    // Sequence
    public static class Sequence implements Writeable, ToXContentObject {
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

        @SuppressWarnings("unchecked")
        public Sequence(StreamInput in) throws IOException {
            this.joinKeys = (List<Object>) in.readGenericValue();
            this.events = in.readCollectionAsList(Event::readFrom);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(joinKeys);
            out.writeCollection(events);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (joinKeys.isEmpty() == false) {
                builder.field(Fields.JOIN_KEYS, joinKeys);
            }
            if (events.isEmpty() == false) {
                builder.startArray(Fields.EVENTS);
                for (Event event : events) {
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
            return Objects.equals(joinKeys, that.joinKeys) && Objects.equals(events, that.events);
        }

        @Override
        public int hashCode() {
            return Objects.hash(joinKeys, events);
        }

        public List<Object> joinKeys() {
            return joinKeys;
        }

        public List<Event> events() {
            return events;
        }
    }

    public static class Hits implements Writeable, ToXContentFragment {
        public static final Hits EMPTY = new Hits(null, null, null);

        private final List<Event> events;
        private final List<Sequence> sequences;
        private final TotalHits totalHits;

        private static final class Fields {
            static final String HITS = "hits";
            static final String TOTAL = "total";
            static final String EVENTS = "events";
            static final String SEQUENCES = "sequences";
        }

        public Hits(@Nullable List<Event> events, @Nullable List<Sequence> sequences, @Nullable TotalHits totalHits) {
            this.events = events;
            this.sequences = sequences;
            this.totalHits = totalHits;
        }

        public Hits(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                totalHits = Lucene.readTotalHits(in);
            } else {
                totalHits = null;
            }
            events = in.readBoolean() ? in.readCollectionAsList(Event::readFrom) : null;
            sequences = in.readBoolean() ? in.readCollectionAsList(Sequence::new) : null;
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
                out.writeCollection(events);
            } else {
                out.writeBoolean(false);
            }
            if (sequences != null) {
                out.writeBoolean(true);
                out.writeCollection(sequences);
            } else {
                out.writeBoolean(false);
            }
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.HITS);
            if (totalHits != null) {
                builder.startObject(Fields.TOTAL);
                builder.field("value", totalHits.value());
                builder.field("relation", totalHits.relation() == TotalHits.Relation.EQUAL_TO ? "eq" : "gte");
                builder.endObject();
            }
            if (events != null) {
                builder.startArray(Fields.EVENTS);
                for (Event event : events) {
                    event.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (sequences != null) {
                builder.field(Fields.SEQUENCES, sequences);
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
                && Objects.equals(totalHits, that.totalHits);
        }

        @Override
        public int hashCode() {
            return Objects.hash(events, sequences, totalHits);
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
    }
}
