/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class ChunkedToXContentBuilderTests extends ESTestCase {

    public void testFieldWithInnerChunkedObject() {

        ToXContent innerXContent = (b, p) -> {
            b.startObject();
            b.field("field1", 10);
            b.field("field2", "aaa");
            b.endObject();
            return b;
        };

        ToXContent outerXContent = (b, p) -> b.field("field3", 10).field("field4", innerXContent);

        String expectedContent = Strings.toString(outerXContent);

        ChunkedToXContentObject innerChunkedContent = params -> new ChunkedToXContentBuilder(params).object(
            o -> o.field("field1", 10).field("field2", "aaa")
        );

        ChunkedToXContent outerChunkedContent = params -> new ChunkedToXContentBuilder(params).field("field3", 10)
            .field("field4", innerChunkedContent);

        assertThat(Strings.toString(outerChunkedContent), equalTo(expectedContent));
    }

    public void testFieldWithInnerChunkedArray() {

        ToXContent innerXContent = (b, p) -> {
            b.startArray();
            b.value(10);
            b.value(20);
            b.endArray();
            return b;
        };

        ToXContent outerXContent = (b, p) -> b.field("field3", 10).field("field4", innerXContent);

        String expectedContent = Strings.toString(outerXContent);

        IntFunction<ToXContent> value = v -> (b, p) -> b.value(v);

        ChunkedToXContentObject innerChunkedContent = params -> new ChunkedToXContentBuilder(params).array(
            IntStream.of(10, 20).mapToObj(value).iterator()
        );

        ChunkedToXContent outerChunkedContent = params -> new ChunkedToXContentBuilder(params).field("field3", 10)
            .field("field4", innerChunkedContent);

        assertThat(Strings.toString(outerChunkedContent), equalTo(expectedContent));
    }

    public void testFieldWithInnerChunkedField() {

        ToXContent innerXContent = (b, p) -> b.value(10);
        ToXContent outerXContent = (b, p) -> b.field("field3", 10).field("field4", innerXContent);

        String expectedContent = Strings.toString(outerXContent);

        ChunkedToXContentObject innerChunkedContent = params -> Iterators.single((b, p) -> b.value(10));

        ChunkedToXContent outerChunkedContent = params -> new ChunkedToXContentBuilder(params).field("field3", 10)
            .field("field4", innerChunkedContent);

        assertThat(Strings.toString(outerChunkedContent), equalTo(expectedContent));
    }
}
