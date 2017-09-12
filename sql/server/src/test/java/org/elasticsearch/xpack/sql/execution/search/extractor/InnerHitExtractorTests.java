/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class InnerHitExtractorTests extends AbstractWireSerializingTestCase<InnerHitExtractor> {
    public static InnerHitExtractor randomInnerHitExtractor() {
        return new InnerHitExtractor(randomAlphaOfLength(5), randomAlphaOfLength(5), randomBoolean());
    }

    @Override
    protected InnerHitExtractor createTestInstance() {
        return randomInnerHitExtractor();
    }

    @Override
    protected Reader<InnerHitExtractor> instanceReader() {
        return InnerHitExtractor::new;
    }

    @Override
    protected InnerHitExtractor mutateInstance(InnerHitExtractor instance) throws IOException {
        return new InnerHitExtractor(instance.hitName() + "mustated", instance.fieldName(), true);
    }

    public void testGet() throws IOException {
        // NOCOMMIT implement after we're sure of the InnerHitExtractor's implementation
    }

    public void testToString() {
        assertEquals("field@hit", new InnerHitExtractor("hit", "field", true).toString());
    }
}
