/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.eql.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class EqlSearchResponseTests extends AbstractBWCSerializationTestCase<EqlSearchResponse> {

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
                BytesReference bytes = new RandomSource(() -> randomAlphaOfLength(10)).toBytes(xType);
                hits.add(new Event(String.valueOf(i), randomAlphaOfLength(10), bytes));
            }
        }
        if (randomBoolean()) {
            return hits;
        }
        return null;
    }

    @Override
    protected EqlSearchResponse createXContextTestInstance(XContentType xContentType) {
        return randomEqlSearchResponse(xContentType);
    }

    @Override
    protected EqlSearchResponse createTestInstance() {
        return randomEqlSearchResponse(XContentType.JSON);
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
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
        } else {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(),
                randomAlphaOfLength(10), randomBoolean(), randomBoolean());
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
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
        } else {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(),
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

    public static EqlSearchResponse createRandomInstance(TotalHits totalHits, XContentType xType) {
        int type = between(0, 1);
        switch(type) {
            case 0:
                return createRandomEventsResponse(totalHits, xType);
            case 1:
                return createRandomSequencesResponse(totalHits, xType);
            default:
                return null;
        }
    }

    @Override
    protected EqlSearchResponse doParseInstance(XContentParser parser) {
        return EqlSearchResponse.fromXContent(parser);
    }
}
