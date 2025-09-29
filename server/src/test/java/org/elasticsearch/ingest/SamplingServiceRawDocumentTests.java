/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class SamplingServiceRawDocumentTests extends AbstractWireSerializingTestCase<SamplingService.RawDocument> {
    @Override
    protected Writeable.Reader<SamplingService.RawDocument> instanceReader() {
        return SamplingService.RawDocument::new;
    }

    @Override
    protected SamplingService.RawDocument createTestInstance() {
        return new SamplingService.RawDocument(
            randomProjectIdOrDefault(),
            randomIdentifier(),
            randomSource(),
            randomFrom(XContentType.values())
        );
    }

    @Override
    protected SamplingService.RawDocument mutateInstance(SamplingService.RawDocument instance) throws IOException {
        return switch (between(0, 3)) {
            case 0 -> new SamplingService.RawDocument(
                randomValueOtherThan(instance.projectId(), ESTestCase::randomProjectIdOrDefault),
                instance.indexName(),
                instance.source(),
                instance.contentType()
            );
            case 1 -> new SamplingService.RawDocument(
                instance.projectId(),
                randomValueOtherThan(instance.indexName(), ESTestCase::randomIdentifier),
                instance.source(),
                instance.contentType()
            );
            case 2 -> new SamplingService.RawDocument(
                instance.projectId(),
                instance.indexName(),
                randomValueOtherThan(instance.source(), SamplingServiceRawDocumentTests::randomSource),
                instance.contentType()
            );
            case 3 -> new SamplingService.RawDocument(
                instance.projectId(),
                instance.indexName(),
                instance.source(),
                randomValueOtherThan(instance.contentType(), () -> randomFrom(XContentType.values()))
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        };
    }

    private static byte[] randomSource() {
        return randomByteArrayOfLength(randomIntBetween(0, 10000));
    }
}
