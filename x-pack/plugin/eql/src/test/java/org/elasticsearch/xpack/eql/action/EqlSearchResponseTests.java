/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.eql.AbstractBWCWireSerializingTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class EqlSearchResponseTests extends AbstractBWCWireSerializingTestCase<EqlSearchResponse> {

    public void testFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values()).canonical();
        EqlSearchResponse response = randomEqlSearchResponse(xContentType);
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(response, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        EqlSearchResponse parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsed = EqlSearchResponse.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
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
            mutatedEvents.add(
                new Event(
                    e.index(),
                    e.id(),
                    e.source(),
                    e.fetchFields(),
                    version.onOrAfter(TransportVersions.V_8_10_X) ? e.missing() : e.index().isEmpty()
                )
            );
        }
        return mutatedEvents;
    }

    public void testEmptyIndexAsMissingEvent() throws IOException {
        Event event = new Event("", "", new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), null, false);
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersions.V_8_9_X);// 8.9.1
        event.writeTo(out);
        ByteArrayStreamInput in = new ByteArrayStreamInput(out.bytes().array());
        in.setTransportVersion(TransportVersions.V_8_9_X);
        Event event2 = Event.readFrom(in);
        assertTrue(event2.missing());
    }
}
