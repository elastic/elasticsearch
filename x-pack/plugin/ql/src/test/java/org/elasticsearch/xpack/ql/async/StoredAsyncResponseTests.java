/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.async;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.async.StoredAsyncResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class StoredAsyncResponseTests extends AbstractWireSerializingTestCase<StoredAsyncResponse<StoredAsyncResponseTests.TestResponse>> {

    public static class TestResponse implements Writeable {
        private final String string;

        public TestResponse(String string) {
            this.string = string;
        }

        public TestResponse(StreamInput input) throws IOException {
            this.string = input.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(string);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestResponse that = (TestResponse) o;
            return Objects.equals(string, that.string);
        }

        @Override
        public int hashCode() {
            return Objects.hash(string);
        }
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected StoredAsyncResponse<TestResponse> createTestInstance() {
        if (randomBoolean()) {
            return new StoredAsyncResponse<>(new IllegalArgumentException(randomAlphaOfLength(10)), randomNonNegativeLong());
        } else {
            return new StoredAsyncResponse<>(new TestResponse(randomAlphaOfLength(10)), randomNonNegativeLong());
        }
    }

    @Override
    protected Writeable.Reader<StoredAsyncResponse<TestResponse>> instanceReader() {
        return in -> new StoredAsyncResponse<>(TestResponse::new, in);
    }
}
