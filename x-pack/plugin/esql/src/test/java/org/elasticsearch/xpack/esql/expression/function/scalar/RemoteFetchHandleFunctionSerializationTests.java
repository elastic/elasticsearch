/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;

public class RemoteFetchHandleFunctionSerializationTests extends ESTestCase {
    public void testSerialization() {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            assertSerialization(
                new RemoteFetchHandleFunction(
                    Source.synthetic("remote_fetch_handle"),
                    randomBoolean()
                        ? new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, randomBoolean())
                        : new ReferenceAttribute(Source.EMPTY, null, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE),
                    randomAlphaOfLength(6),
                    randomAlphaOfLength(8)
                )
            );
        }
    }
}
