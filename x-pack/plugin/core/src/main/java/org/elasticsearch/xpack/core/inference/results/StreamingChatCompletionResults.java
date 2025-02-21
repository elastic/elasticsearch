/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeEquals;
import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeHashCode;
import static org.elasticsearch.xpack.core.inference.DequeUtils.readDeque;
import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResults.COMPLETION;

/**
 * Chat Completion results that only contain a Flow.Publisher.
 */
public record StreamingChatCompletionResults(Flow.Publisher<? extends InferenceServiceResults.Result> publisher)
    implements
        InferenceServiceResults {

    @Override
    public boolean isStreaming() {
        return true;
    }

    public record Results(Deque<Result> results) implements InferenceServiceResults.Result {
        public static final String NAME = "streaming_chat_completion_results";

        public Results(StreamInput in) throws IOException {
            this(readDeque(in, Result::new));
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.startArray(COMPLETION),
                Iterators.flatMap(results.iterator(), d -> d.toXContentChunked(params)),
                ChunkedToXContentHelper.endArray(),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(results);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Results other = (Results) o;
            return dequeEquals(this.results, other.results());
        }

        @Override
        public int hashCode() {
            return dequeHashCode(results);
        }
    }

    public record Result(String delta) implements ChunkedToXContent, Writeable {
        private static final String RESULT = "delta";

        private Result(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((b, p) -> b.startObject().field(RESULT, delta).endObject());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(delta);
        }
    }
}
