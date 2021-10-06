/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.eql;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EqlSearchResponseTests extends AbstractResponseTestCase<org.elasticsearch.xpack.eql.action.EqlSearchResponse,
    EqlSearchResponse> {

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

    static List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event> randomEvents(XContentType xType) {
        int size = randomIntBetween(1, 10);
        List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event> hits = null;
        if (randomBoolean()) {
            hits = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                BytesReference bytes = new RandomSource(() -> randomAlphaOfLength(10)).toBytes(xType);
                Map<String, DocumentField> fetchFields = new HashMap<>();
                int fieldsCount = randomIntBetween(0, 5);
                for (int j = 0; j < fieldsCount; j++) {
                    fetchFields.put(randomAlphaOfLength(10), randomDocumentField(xType).v1());
                }
                if (fetchFields.isEmpty() && randomBoolean()) {
                    fetchFields = null;
                }
                hits.add(new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event(String.valueOf(i), randomAlphaOfLength(10), bytes,
                    fetchFields));
            }
        }
        if (randomBoolean()) {
            return null;
        }
        return hits;
    }

    private static Tuple<DocumentField, DocumentField> randomDocumentField(XContentType xType) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                String fieldName = randomAlphaOfLengthBetween(3, 10);
                Tuple<List<Object>, List<Object>> tuple = RandomObjects.randomStoredFieldValues(random(), xType);
                DocumentField input = new DocumentField(fieldName, tuple.v1());
                DocumentField expected = new DocumentField(fieldName, tuple.v2());
                return Tuple.tuple(input, expected);
            case 1:
                List<Object> listValues = randomList(1, 5, () -> randomList(1, 5, ESTestCase::randomInt));
                DocumentField listField = new DocumentField(randomAlphaOfLength(5), listValues);
                return Tuple.tuple(listField, listField);
            case 2:
                List<Object> objectValues = randomList(1, 5, () ->
                    Map.of(randomAlphaOfLength(5), randomInt(),
                        randomAlphaOfLength(5), randomBoolean(),
                        randomAlphaOfLength(5), randomAlphaOfLength(10)));
                DocumentField objectField = new DocumentField(randomAlphaOfLength(5), objectValues);
                return Tuple.tuple(objectField, objectField);
            default:
                throw new IllegalStateException();
        }
    }

    public static org.elasticsearch.xpack.eql.action.EqlSearchResponse createRandomEventsResponse(TotalHits totalHits, XContentType xType) {
        org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits(randomEvents(xType), null, totalHits);
        }
        if (randomBoolean()) {
            return new org.elasticsearch.xpack.eql.action.EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
        } else {
            return new org.elasticsearch.xpack.eql.action.EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(),
                randomAlphaOfLength(10), randomBoolean(), randomBoolean());
        }
    }

    public static org.elasticsearch.xpack.eql.action.EqlSearchResponse createRandomSequencesResponse(TotalHits totalHits,
                                                                                                     XContentType xType) {
        int size = randomIntBetween(1, 10);
        List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence> seq = null;
        if (randomBoolean()) {
            List<Supplier<Object[]>> randoms = getKeysGenerators();
            seq = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                List<Object> joins = null;
                if (randomBoolean()) {
                    joins = Arrays.asList(randomFrom(randoms).get());
                }
                seq.add(new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence(joins, randomEvents(xType)));
            }
        }
        org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits(null, seq, totalHits);
        }
        if (randomBoolean()) {
            return new org.elasticsearch.xpack.eql.action.EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
        } else {
            return new org.elasticsearch.xpack.eql.action.EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(),
                randomAlphaOfLength(10), randomBoolean(), randomBoolean());
        }
    }

    private static List<Supplier<Object[]>> getKeysGenerators() {
        List<Supplier<Object[]>> randoms = new ArrayList<>();
        randoms.add(() -> generateRandomStringArray(6, 11, false));
        randoms.add(() -> randomArray(0, 6, Integer[]::new, ()-> randomInt()));
        randoms.add(() -> randomArray(0, 6, Long[]::new, ()-> randomLong()));
        randoms.add(() -> randomArray(0, 6, Boolean[]::new, ()-> randomBoolean()));

        return randoms;
    }

    public static org.elasticsearch.xpack.eql.action.EqlSearchResponse createRandomInstance(TotalHits totalHits, XContentType xType) {
        int type = between(0, 1);
        switch (type) {
            case 0:
                return createRandomEventsResponse(totalHits, xType);
            case 1:
                return createRandomSequencesResponse(totalHits, xType);
            default:
                return null;
        }
    }

    @Override
    protected org.elasticsearch.xpack.eql.action.EqlSearchResponse createServerTestInstance(XContentType xContentType) {
        TotalHits totalHits = null;
        if (randomBoolean()) {
            totalHits = new TotalHits(randomIntBetween(100, 1000), TotalHits.Relation.EQUAL_TO);
        }
        return createRandomInstance(totalHits, xContentType);
    }

    @Override
    protected EqlSearchResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return EqlSearchResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.eql.action.EqlSearchResponse serverTestInstance, EqlSearchResponse clientInstance) {
        assertThat(serverTestInstance.took(), is(clientInstance.took()));
        assertThat(serverTestInstance.isTimeout(), is(clientInstance.isTimeout()));
        assertThat(serverTestInstance.hits().totalHits(), is(clientInstance.hits().totalHits()));
        if (serverTestInstance.hits().events() == null) {
            assertNull(clientInstance.hits().events());
        } else {
            assertEvents(serverTestInstance.hits().events(), clientInstance.hits().events());
        }
        if (serverTestInstance.hits().sequences() == null) {
            assertNull(clientInstance.hits().sequences());
        } else {
            assertThat(serverTestInstance.hits().sequences().size(), equalTo(clientInstance.hits().sequences().size()));
            for (int i = 0; i < serverTestInstance.hits().sequences().size(); i++) {
                assertThat(serverTestInstance.hits().sequences().get(i).joinKeys(),
                    is(clientInstance.hits().sequences().get(i).joinKeys()));
                assertEvents(serverTestInstance.hits().sequences().get(i).events(), clientInstance.hits().sequences().get(i).events());
            }
        }
    }

    private void assertEvents(
        List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event> serverEvents,
        List<EqlSearchResponse.Event> clientEvents
    ) {
        assertThat(serverEvents.size(), equalTo(clientEvents.size()));
        for (int j = 0; j < serverEvents.size(); j++) {
            assertThat(
                SourceLookup.sourceAsMap(serverEvents.get(j).source()), is(clientEvents.get(j).sourceAsMap()));
        }
    }
}
