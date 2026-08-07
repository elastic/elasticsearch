/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class RemoteFetchHandleFunctionSerializationTests extends AbstractExpressionSerializationTests<RemoteFetchHandleFunction> {
    /**
     * Keep this registration test-local for now. Production registration is intentionally deferred
     * until remote fetch handles need wire shipping, so this PR does not take that compatibility
     * commitment yet.
     */
    @Override
    protected List<NamedWriteableRegistry.Entry> extraNamedWriteables() {
        return List.of(RemoteFetchHandleFunction.ENTRY);
    }

    @Override
    protected RemoteFetchHandleFunction createTestInstance() {
        return new RemoteFetchHandleFunction(randomSource(), randomDocAttribute(), randomAlphaOfLength(6), randomAlphaOfLength(8));
    }

    @Override
    protected RemoteFetchHandleFunction mutateInstance(RemoteFetchHandleFunction instance) throws IOException {
        Attribute doc = instance.doc();
        String nodeId = instance.nodeId();
        String retainedSessionId = instance.retainedSessionId();
        switch (between(0, 2)) {
            case 0 -> doc = randomValueOtherThan(doc, RemoteFetchHandleFunctionSerializationTests::randomDocAttribute);
            case 1 -> nodeId = randomValueOtherThan(nodeId, () -> randomAlphaOfLength(6));
            case 2 -> retainedSessionId = randomValueOtherThan(retainedSessionId, () -> randomAlphaOfLength(8));
            default -> throw new IllegalStateException("unexpected mutation branch");
        }
        return new RemoteFetchHandleFunction(instance.source(), doc, nodeId, retainedSessionId);
    }

    private static Attribute randomDocAttribute() {
        return randomBoolean()
            ? new MetadataAttribute(randomSource(), MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, randomBoolean())
            : new ReferenceAttribute(randomSource(), null, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE);
    }
}
