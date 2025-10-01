/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.SamplingService.RawDocument;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class SamplingServiceRawDocumentTests extends AbstractWireSerializingTestCase<RawDocument> {
    @Override
    protected Writeable.Reader<RawDocument> instanceReader() {
        return RawDocument::new;
    }

    @Override
    protected RawDocument createTestInstance() {
        return new RawDocument(
            randomProjectIdOrDefault(),
            randomIdentifier(),
            randomByteArrayOfLength(randomIntBetween(10, 1000)),
            randomFrom(XContentType.values())
        );
    }

    @Override
    protected RawDocument mutateInstance(RawDocument instance) throws IOException {
        ProjectId projectId = instance.projectId();
        String indexName = instance.indexName();
        byte[] source = instance.source();
        XContentType xContentType = instance.contentType();

        switch (between(0, 3)) {
            case 0 -> projectId = randomValueOtherThan(projectId, ESTestCase::randomProjectIdOrDefault);
            case 1 -> indexName = randomValueOtherThan(indexName, ESTestCase::randomIdentifier);
            case 2 -> source = randomByteArrayOfLength(randomIntBetween(100, 1000));
            case 3 -> xContentType = randomValueOtherThan(xContentType, () -> randomFrom(XContentType.values()));
            default -> throw new IllegalArgumentException("Should never get here");
        }
        return new RawDocument(projectId, indexName, source, xContentType);
    }
}
