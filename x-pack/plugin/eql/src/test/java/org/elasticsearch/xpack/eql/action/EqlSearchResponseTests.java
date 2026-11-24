/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.eql.AbstractBWCWireSerializingTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;

public class EqlSearchResponseTests extends AbstractBWCWireSerializingTestCase<EqlSearchResponse> {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Event, Void> EVENT_PARSER = new ConstructingObjectParser<>(
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
        EVENT_PARSER.declareString(constructorArg(), new ParseField("_index"));
        EVENT_PARSER.declareString(constructorArg(), new ParseField("_id"));
        EVENT_PARSER.declareObject(constructorArg(), (p, c) -> {
            try (XContentBuilder builder = XContentBuilder.builder(p.contentType().xContent())) {
                builder.copyCurrentStructure(p);
                return ReleasableBytesReference.wrap(BytesReference.bytes(builder));
            }
        }, new ParseField("_source"));
        EVENT_PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            Map<String, DocumentField> fields = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                DocumentField field = DocumentField.fromXContent(p);
                fields.put(field.getName(), field);
            }
            return fields;
        }, new ParseField("fields"));
        EVENT_PARSER.declareBoolean(optionalConstructorArg(), new ParseField("missing"));
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Sequence, Void> SEQUENCE_PARSER = new ConstructingObjectParser<>(
        "eql/search_response_sequence",
        true,
        args -> new Sequence((List<Object>) args[0], (List<Event>) args[1])
    );

    static {
        SEQUENCE_PARSER.declareFieldArray(
            optionalConstructorArg(),
            (p, c) -> XContentParserUtils.parseFieldsValue(p),
            new ParseField("join_keys"),
            ObjectParser.ValueType.VALUE_ARRAY
        );
        SEQUENCE_PARSER.declareObjectArray(optionalConstructorArg(), EVENT_PARSER, new ParseField("events"));
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EqlSearchResponse.Hits, Void> HITS_PARSER = new ConstructingObjectParser<>(
        "eql/search_response_hits",
        true,
        args -> new EqlSearchResponse.Hits((List<Event>) args[0], (List<Sequence>) args[1], (TotalHits) args[2])
    );

    static {
        HITS_PARSER.declareObjectArray(optionalConstructorArg(), EVENT_PARSER, new ParseField("events"));
        HITS_PARSER.declareObjectArray(optionalConstructorArg(), SEQUENCE_PARSER, new ParseField("sequences"));
        HITS_PARSER.declareObject(optionalConstructorArg(), (p, c) -> SearchHits.parseTotalHitsFragment(p), new ParseField("total"));
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EqlSearchResponse, Void> RESPONSE_PARSER = new ConstructingObjectParser<>(
        "eql/search_response",
        true,
        args -> new EqlSearchResponse(
            (EqlSearchResponse.Hits) args[0],
            (long) args[1],
            (boolean) args[2],
            (String) args[3],
            (boolean) args[4],
            (boolean) args[5],
            Objects.requireNonNullElse((List<ShardSearchFailure>) args[6], List.<ShardSearchFailure>of())
                .toArray(ShardSearchFailure.EMPTY_ARRAY)
        )
    );

    static {
        RESPONSE_PARSER.declareObject(constructorArg(), HITS_PARSER, new ParseField("hits"));
        RESPONSE_PARSER.declareLong(constructorArg(), new ParseField("took"));
        RESPONSE_PARSER.declareBoolean(constructorArg(), new ParseField("timed_out"));
        RESPONSE_PARSER.declareString(optionalConstructorArg(), new ParseField("id"));
        RESPONSE_PARSER.declareBoolean(constructorArg(), new ParseField("is_running"));
        RESPONSE_PARSER.declareBoolean(constructorArg(), new ParseField("is_partial"));
        RESPONSE_PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> SearchResponseUtils.parseShardSearchFailure(p),
            new ParseField("shard_failures")
        );
    }

    @Override
    protected void dispose(EqlSearchResponse instance) {
        if (instance != null) {
            instance.decRef();
        }
    }

    public void testFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values()).canonical();
        EqlSearchResponse response = randomEqlSearchResponse(xContentType);
        try {
            boolean humanReadable = randomBoolean();
            BytesReference originalBytes = toShuffledXContent(response, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
            EqlSearchResponse parsed;
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                parsed = RESPONSE_PARSER.apply(parser, null);
            }
            try {
                assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
            } finally {
                parsed.decRef();
            }
        } finally {
            response.decRef();
        }
    }

    /**
     * Parsed shard failures wrap the original cause in a generic {@link org.elasticsearch.ElasticsearchException},
     * so the XContent does not round-trip exactly. Compare the shard targets and cause messages instead.
     */
    public void testFromXContentWithShardFailures() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values()).canonical();
        ShardSearchFailure[] shardFailures = new ShardSearchFailure[randomIntBetween(1, 5)];
        for (int i = 0; i < shardFailures.length; i++) {
            SearchShardTarget target = randomBoolean()
                ? null
                : new SearchShardTarget(
                    randomAlphaOfLengthBetween(5, 10),
                    new ShardId(new Index(randomAlphaOfLengthBetween(5, 10), IndexMetadata.INDEX_UUID_NA_VALUE), randomNonNegativeInt()),
                    randomBoolean() ? randomAlphaOfLengthBetween(5, 10) : null
                );
            shardFailures[i] = new ShardSearchFailure(new IllegalArgumentException("failure-" + i), target);
        }
        EqlSearchResponse response = new EqlSearchResponse(
            new EqlSearchResponse.Hits(null, null, null),
            randomNonNegativeLong(),
            randomBoolean(),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean(),
            randomBoolean(),
            shardFailures
        );
        try {
            BytesReference bytes = toShuffledXContent(response, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
            EqlSearchResponse parsed;
            try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
                parsed = RESPONSE_PARSER.apply(parser, null);
            }
            try {
                assertThat(parsed.took(), equalTo(response.took()));
                assertThat(parsed.isTimeout(), equalTo(response.isTimeout()));
                assertThat(parsed.id(), equalTo(response.id()));
                assertThat(parsed.isRunning(), equalTo(response.isRunning()));
                assertThat(parsed.isPartial(), equalTo(response.isPartial()));
                assertThat(parsed.shardFailures(), arrayWithSize(shardFailures.length));
                for (int i = 0; i < shardFailures.length; i++) {
                    ShardSearchFailure expected = shardFailures[i];
                    ShardSearchFailure actual = parsed.shardFailures()[i];
                    assertThat(actual.shard(), equalTo(expected.shard()));
                    assertThat(
                        actual.getCause().getMessage(),
                        equalTo("Elasticsearch exception [type=illegal_argument_exception, reason=failure-" + i + "]")
                    );
                }
            } finally {
                parsed.decRef();
            }
        } finally {
            response.decRef();
        }
    }

    private static class RandomSource implements ToXContentObject {

        private final String key;
        private final String value;

        RandomSource(Supplier<String> randomStringSupplier) {
            this.key = randomStringSupplier.get();
            this.value = randomStringSupplier.get();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(key, value);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            RandomSource other = (RandomSource) obj;
            return Objects.equals(key, other.key) && Objects.equals(value, other.value);
        }

        public BytesReference toBytes(XContentType type) {
            try (XContentBuilder builder = XContentBuilder.builder(type.xContent())) {
                toXContent(builder, ToXContent.EMPTY_PARAMS);
                return BytesReference.bytes(builder);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    static List<Event> randomEvents(XContentType xType) {
        int size = randomIntBetween(1, 10);
        List<Event> hits = null;
        if (randomBoolean()) {
            hits = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                if (randomBoolean()) {
                    hits.add(Event.MISSING_EVENT);
                } else {
                    BytesReference bytes = new RandomSource(() -> randomAlphaOfLength(10)).toBytes(xType);
                    Map<String, DocumentField> fetchFields = new HashMap<>();
                    int fieldsCount = randomIntBetween(0, 5);
                    for (int j = 0; j < fieldsCount; j++) {
                        fetchFields.put(randomAlphaOfLength(10), randomDocumentField(xType).v1());
                    }
                    if (fetchFields.isEmpty() && randomBoolean()) {
                        fetchFields = null;
                    }
                    hits.add(new Event(String.valueOf(i), randomAlphaOfLength(10), bytes, fetchFields, false));
                }
            }
        }
        if (randomBoolean()) {
            return hits;
        }
        return null;
    }

    private static Tuple<DocumentField, DocumentField> randomDocumentField(XContentType xType) {
        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                String fieldName = randomAlphaOfLengthBetween(3, 10);
                Tuple<List<Object>, List<Object>> tuple = RandomObjects.randomStoredFieldValues(random(), xType);
                DocumentField input = new DocumentField(fieldName, tuple.v1());
                DocumentField expected = new DocumentField(fieldName, tuple.v2());
                return Tuple.tuple(input, expected);
            }
            case 1 -> {
                List<Object> listValues = randomList(1, 5, () -> randomList(1, 5, ESTestCase::randomInt));
                DocumentField listField = new DocumentField(randomAlphaOfLength(5), listValues);
                return Tuple.tuple(listField, listField);
            }
            case 2 -> {
                List<Object> objectValues = randomList(
                    1,
                    5,
                    () -> Map.of(
                        randomAlphaOfLength(5),
                        randomInt(),
                        randomAlphaOfLength(6),
                        randomBoolean(),
                        randomAlphaOfLength(7),
                        randomAlphaOfLength(10)
                    )
                );
                DocumentField objectField = new DocumentField(randomAlphaOfLength(5), objectValues);
                return Tuple.tuple(objectField, objectField);
            }
            default -> throw new IllegalStateException();
        }
    }

    @Override
    protected EqlSearchResponse createTestInstance() {
        return randomEqlSearchResponse(XContentType.JSON);
    }

    @Override
    protected EqlSearchResponse mutateInstance(EqlSearchResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<EqlSearchResponse> instanceReader() {
        return EqlSearchResponse::new;
    }

    public void testDirectoryMetricsSerializationRoundTrip() throws IOException {
        long bytesRead = randomNonNegativeLong();
        EqlSearchResponse response = createTestInstance();
        response.setDirectoryMetrics(storeMetrics(bytesRead));
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(DirectoryMetrics.PluggableMetrics.class, StoreMetrics.NAME, StoreMetrics::new))
        );
        try {
            EqlSearchResponse deserialized = copyWriteable(response, registry, EqlSearchResponse::new, TransportVersion.current());
            try {
                assertEquals(bytesRead, deserialized.directoryMetrics().metrics(StoreMetrics.NAME).cast(StoreMetrics.class).getBytesRead());
            } finally {
                deserialized.decRef();
            }
        } finally {
            response.decRef();
        }
    }

    private static DirectoryMetrics storeMetrics(long bytesRead) {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, new StoreMetrics(bytesRead));
        return builder.build();
    }

    public static EqlSearchResponse randomEqlSearchResponse(XContentType xContentType) {
        TotalHits totalHits = null;
        if (randomBoolean()) {
            totalHits = new TotalHits(randomIntBetween(100, 1000), TotalHits.Relation.EQUAL_TO);
        }
        return createRandomInstance(totalHits, xContentType);
    }

    public static EqlSearchResponse createRandomEventsResponse(TotalHits totalHits, XContentType xType) {
        EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new EqlSearchResponse.Hits(randomEvents(xType), null, totalHits);
        }
        if (randomBoolean()) {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(), ShardSearchFailure.EMPTY_ARRAY);
        } else {
            return new EqlSearchResponse(
                hits,
                randomIntBetween(0, 1001),
                randomBoolean(),
                randomAlphaOfLength(10),
                randomBoolean(),
                randomBoolean(),
                ShardSearchFailure.EMPTY_ARRAY
            );
        }
    }

    public static EqlSearchResponse createRandomSequencesResponse(TotalHits totalHits, XContentType xType) {
        int size = randomIntBetween(1, 10);
        List<EqlSearchResponse.Sequence> seq = null;
        if (randomBoolean()) {
            List<Supplier<Object[]>> randoms = getKeysGenerators();
            seq = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                List<Object> joins = null;
                if (randomBoolean()) {
                    joins = Arrays.asList(randomFrom(randoms).get());
                }
                seq.add(new EqlSearchResponse.Sequence(joins, randomEvents(xType)));
            }
        }
        EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new EqlSearchResponse.Hits(null, seq, totalHits);
        }
        if (randomBoolean()) {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(), ShardSearchFailure.EMPTY_ARRAY);
        } else {
            return new EqlSearchResponse(
                hits,
                randomIntBetween(0, 1001),
                randomBoolean(),
                randomAlphaOfLength(10),
                randomBoolean(),
                randomBoolean(),
                ShardSearchFailure.EMPTY_ARRAY
            );
        }
    }

    private static List<Supplier<Object[]>> getKeysGenerators() {
        List<Supplier<Object[]>> randoms = new ArrayList<>();
        randoms.add(() -> generateRandomStringArray(6, 11, false));
        randoms.add(() -> randomArray(0, 6, Integer[]::new, () -> randomInt()));
        randoms.add(() -> randomArray(0, 6, Long[]::new, () -> randomLong()));
        randoms.add(() -> randomArray(0, 6, Boolean[]::new, () -> randomBoolean()));

        return randoms;
    }

    public static EqlSearchResponse createRandomInstance(TotalHits totalHits, XContentType xType) {
        int type = between(0, 1);
        return switch (type) {
            case 0 -> createRandomEventsResponse(totalHits, xType);
            case 1 -> createRandomSequencesResponse(totalHits, xType);
            default -> null;
        };
    }

    @Override
    protected EqlSearchResponse mutateInstanceForVersion(EqlSearchResponse instance, TransportVersion version) {
        List<Event> mutatedEvents = mutateEvents(instance.hits().events(), version);

        List<Sequence> sequences = instance.hits().sequences();
        List<Sequence> mutatedSequences = null;
        if (sequences != null) {
            mutatedSequences = new ArrayList<>(sequences.size());
            for (Sequence s : sequences) {
                mutatedSequences.add(new Sequence(s.joinKeys(), mutateEvents(s.events(), version)));
            }
        }

        return new EqlSearchResponse(
            new EqlSearchResponse.Hits(mutatedEvents, mutatedSequences, instance.hits().totalHits()),
            instance.took(),
            instance.isTimeout(),
            instance.id(),
            instance.isRunning(),
            instance.isPartial(),
            ShardSearchFailure.EMPTY_ARRAY
        );
    }

    private List<Event> mutateEvents(List<Event> original, TransportVersion version) {
        if (original == null || original.isEmpty()) {
            return original;
        }
        List<Event> mutatedEvents = new ArrayList<>(original.size());
        for (Event e : original) {
            mutatedEvents.add(new Event(e.index(), e.id(), e.source(), e.fetchFields(), e.missing()));
        }
        return mutatedEvents;
    }
}
