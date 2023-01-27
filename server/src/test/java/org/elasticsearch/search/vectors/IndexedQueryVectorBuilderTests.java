/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IndexedQueryVectorBuilderTests extends AbstractQueryVectorBuilderTestCase<IndexedQueryVectorBuilder> {

    static IndexedQueryVectorBuilder getRandomInstance() {
        return new IndexedQueryVectorBuilder(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    @Override
    protected Writeable.Reader<IndexedQueryVectorBuilder> instanceReader() {
        return IndexedQueryVectorBuilder::new;
    }

    @Override
    protected IndexedQueryVectorBuilder createTestInstance() {
        return getRandomInstance();
    }

    @Override
    protected IndexedQueryVectorBuilder mutateInstance(IndexedQueryVectorBuilder instance) throws IOException {
        int i = randomInt(3);
        return switch (i) {
            case 0 -> new IndexedQueryVectorBuilder(randomAlphaOfLength(10), instance.getId(), instance.getPath(), instance.getRouting());
            case 1 -> new IndexedQueryVectorBuilder(
                instance.getIndex(),
                randomAlphaOfLength(10),
                instance.getPath(),
                instance.getRouting()
            );
            case 2 -> new IndexedQueryVectorBuilder(instance.getIndex(), instance.getId(), randomAlphaOfLength(10), instance.getRouting());
            case 3 -> new IndexedQueryVectorBuilder(
                instance.getIndex(),
                instance.getId(),
                instance.getPath(),
                instance.getRouting() == null ? randomAlphaOfLength(10) : null
            );
            default -> throw new IllegalStateException("Unexpected value: " + i);
        };
    }

    @Override
    protected IndexedQueryVectorBuilder doParseInstance(XContentParser parser) throws IOException {
        return IndexedQueryVectorBuilder.fromXContent(parser);
    }

    @Override
    protected void doAssertClientRequest(ActionRequest request, IndexedQueryVectorBuilder builder) {
        assertThat(request, instanceOf(GetRequest.class));
        GetRequest getRequest = (GetRequest) request;
        assertThat(getRequest.id(), equalTo(builder.getId()));
        assertThat(getRequest.index(), equalTo(builder.getIndex()));
        assertThat(getRequest.routing(), equalTo(builder.getRouting()));
        assertThat(getRequest.preference(), equalTo("_local"));
    }

    @Override
    protected ActionResponse createResponse(float[] array, IndexedQueryVectorBuilder builder) {
        return new GetResponse(
            new GetResult(
                "index",
                "id",
                1,
                1,
                1,
                true,
                new BytesArray(Strings.format("{\"%s\": %s}", builder.getPath(), Arrays.toString(array))),
                Map.of(),
                Map.of()
            )
        );
    }
}
