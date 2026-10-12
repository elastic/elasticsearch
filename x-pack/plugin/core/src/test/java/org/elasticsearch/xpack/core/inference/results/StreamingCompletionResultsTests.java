/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayDeque;

public class StreamingCompletionResultsTests extends AbstractWireSerializingTestCase<StreamingCompletionResults.Results> {
    @Override
    protected Writeable.Reader<StreamingCompletionResults.Results> instanceReader() {
        return StreamingCompletionResults.Results::new;
    }

    @Override
    protected StreamingCompletionResults.Results createTestInstance() {
        var results = new ArrayDeque<StreamingCompletionResults.Result>();
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            results.offer(new StreamingCompletionResults.Result(randomAlphanumericOfLength(5)));
        }
        return new StreamingCompletionResults.Results(results);
    }

    @Override
    protected StreamingCompletionResults.Results mutateInstance(StreamingCompletionResults.Results instance) throws IOException {
        var results = new ArrayDeque<>(instance.results());
        if (randomBoolean()) {
            results.pop();
        } else {
            results.offer(new StreamingCompletionResults.Result(randomAlphanumericOfLength(5)));
        }
        return new StreamingCompletionResults.Results(results);
    }
}
