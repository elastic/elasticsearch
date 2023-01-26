/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QueryPageTests extends AbstractWireSerializingTestCase<QueryPage<QueryPageTests.QueryPageTester>> {

    static class QueryPageTester implements ToXContentObject, Writeable {

        private final String someString;
        private final long someLong;

        QueryPageTester(String someString, long someLong) {
            this.someString = someString;
            this.someLong = someLong;
        }

        QueryPageTester(StreamInput in) throws IOException {
            this.someString = in.readString();
            this.someLong = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(someString);
            out.writeLong(someLong);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("some_string", someString);
            builder.field("some_long", someLong);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            QueryPageTester that = (QueryPageTester) other;
            return Objects.equals(that.someString, someString) && that.someLong == someLong;
        }

        @Override
        public int hashCode() {
            return Objects.hash(someString, someLong);
        }
    }

    @Override
    protected QueryPage<QueryPageTests.QueryPageTester> createTestInstance() {
        int hitCount = randomIntBetween(0, 10);
        ArrayList<QueryPageTests.QueryPageTester> hits = new ArrayList<>();
        for (int i = 0; i < hitCount; i++) {
            hits.add(new QueryPageTests.QueryPageTester(randomAlphaOfLength(10), randomLong()));
        }
        return new QueryPage<>(hits, hitCount, new ParseField("test"));
    }

    @Override
    protected Reader<QueryPage<QueryPageTests.QueryPageTester>> instanceReader() {
        return (in) -> new QueryPage<>(in, QueryPageTests.QueryPageTester::new);
    }

    @Override
    protected QueryPage<QueryPageTests.QueryPageTester> mutateInstance(QueryPage<QueryPageTester> instance) {
        ParseField resultsField = instance.getResultsField();
        List<QueryPageTests.QueryPageTester> page = instance.results();
        long count = instance.count();
        switch (between(0, 1)) {
            case 0 -> {
                page = new ArrayList<>(page);
                page.add(new QueryPageTester(randomAlphaOfLength(10), randomLong()));
            }
            case 1 -> count += between(1, 20);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new QueryPage<>(page, count, resultsField);
    }
}
