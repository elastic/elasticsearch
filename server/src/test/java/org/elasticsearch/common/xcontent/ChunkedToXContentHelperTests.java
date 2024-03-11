/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;
import java.util.function.IntFunction;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class ChunkedToXContentHelperTests extends ESTestCase {

    public void testFieldWithInnerChunkedObject() {

        ToXContent innerXContent = (builder, p) -> {
            builder.startObject();
            builder.field("field1", 10);
            builder.field("field2", "aaa");
            builder.endObject();
            return builder;
        };

        ToXContent outerXContent = (builder, p) -> {
            builder.field("field3", 10);
            builder.field("field4", innerXContent);
            return builder;
        };

        var expectedContent = Strings.toString(outerXContent);

        ChunkedToXContentObject innerChunkedContent = params -> Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            ChunkedToXContentHelper.field("field1", 10),
            ChunkedToXContentHelper.field("field2", "aaa"),
            ChunkedToXContentHelper.endObject()
        );

        ChunkedToXContent outerChunkedContent = params -> Iterators.concat(
            ChunkedToXContentHelper.field("field3", 10),
            ChunkedToXContentHelper.field("field4", innerChunkedContent, EMPTY_PARAMS)
        );

        assertThat(Strings.toString(outerChunkedContent), equalTo(expectedContent));
    }

    public void testFieldWithInnerChunkedArray() {

        ToXContent innerXContent = (builder, p) -> {
            builder.startArray();
            builder.value(10);
            builder.value(20);
            builder.endArray();
            return builder;
        };

        ToXContent outerXContent = (builder, p) -> {
            builder.field("field3", 10);
            builder.field("field4", innerXContent);
            return builder;
        };

        var expectedContent = Strings.toString(outerXContent);

        IntFunction<Iterator<ToXContent>> value = v -> Iterators.single(((builder, p) -> builder.value(v)));

        ChunkedToXContentObject innerChunkedContent = params -> Iterators.concat(
            ChunkedToXContentHelper.startArray(),
            value.apply(10),
            value.apply(20),
            ChunkedToXContentHelper.endArray()
        );

        ChunkedToXContent outerChunkedContent = params -> Iterators.concat(
            ChunkedToXContentHelper.field("field3", 10),
            ChunkedToXContentHelper.field("field4", innerChunkedContent, EMPTY_PARAMS)
        );

        assertThat(Strings.toString(outerChunkedContent), equalTo(expectedContent));
    }

    public void testFieldWithInnerChunkedField() {

        ToXContent innerXContent = (builder, p) -> {
            builder.value(10);
            return builder;
        };

        ToXContent outerXContent = (builder, p) -> {
            builder.field("field3", 10);
            builder.field("field4", innerXContent);
            return builder;
        };

        var expectedContent = Strings.toString(outerXContent);

        IntFunction<Iterator<ToXContent>> value = v -> Iterators.single(((builder, p) -> builder.value(v)));

        ChunkedToXContentObject innerChunkedContent = params -> Iterators.single((builder, p) -> builder.value(10));

        ChunkedToXContent outerChunkedContent = params -> Iterators.concat(
            ChunkedToXContentHelper.field("field3", 10),
            ChunkedToXContentHelper.field("field4", innerChunkedContent, EMPTY_PARAMS)
        );

        assertThat(Strings.toString(outerChunkedContent), equalTo(expectedContent));
    }
}
