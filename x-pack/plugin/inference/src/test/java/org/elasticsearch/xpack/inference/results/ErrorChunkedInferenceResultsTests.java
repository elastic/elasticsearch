/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;

import java.io.IOException;

public class ErrorChunkedInferenceResultsTests extends AbstractWireSerializingTestCase<ErrorChunkedInferenceResults> {

    public static ErrorChunkedInferenceResults createRandomResults() {
        return new ErrorChunkedInferenceResults(
            randomBoolean()
                ? new ElasticsearchTimeoutException(randomAlphaOfLengthBetween(10, 50))
                : new ElasticsearchStatusException(randomAlphaOfLengthBetween(10, 50), randomFrom(RestStatus.values()))
        );
    }

    @Override
    protected Writeable.Reader<ErrorChunkedInferenceResults> instanceReader() {
        return ErrorChunkedInferenceResults::new;
    }

    @Override
    protected ErrorChunkedInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected ErrorChunkedInferenceResults mutateInstance(ErrorChunkedInferenceResults instance) throws IOException {
        return new ErrorChunkedInferenceResults(new RuntimeException(randomAlphaOfLengthBetween(10, 50)));
    }
}
