/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class JsonRemoveSerializationTests extends AbstractExpressionSerializationTests<JsonRemove> {
    public void testRejectsUnsupportedRecipient() {
        var expression = createTestInstance();
        var version = TransportVersionUtils.getPreviousVersion(FieldAttribute.ESQL_PROMQL_LABEL_RECORD);
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            IOException error = expectThrows(IOException.class, () -> expression.writeTo(out));
            assertEquals("JSON object edits are not supported by the recipient", error.getMessage());
        }
    }

    @Override
    protected JsonRemove createTestInstance() {
        return new JsonRemove(randomSource(), randomChild(), List.of(randomAlphaOfLength(5), randomAlphaOfLength(7)));
    }

    @Override
    protected JsonRemove mutateInstance(JsonRemove instance) {
        return randomBoolean()
            ? new JsonRemove(
                instance.source(),
                randomValueOtherThan(instance.children().getFirst(), AbstractExpressionSerializationTests::randomChild),
                instance.fields()
            )
            : new JsonRemove(
                instance.source(),
                instance.children().getFirst(),
                List.of(randomValueOtherThanMany(instance.fields()::contains, () -> randomAlphaOfLength(5)))
            );
    }
}
