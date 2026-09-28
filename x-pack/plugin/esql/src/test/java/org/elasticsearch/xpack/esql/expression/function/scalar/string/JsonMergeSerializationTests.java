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

public class JsonMergeSerializationTests extends AbstractExpressionSerializationTests<JsonMerge> {
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
    protected JsonMerge createTestInstance() {
        return new JsonMerge(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected JsonMerge mutateInstance(JsonMerge instance) {
        return randomBoolean()
            ? new JsonMerge(
                instance.source(),
                randomValueOtherThan(instance.children().get(0), AbstractExpressionSerializationTests::randomChild),
                instance.children().get(1)
            )
            : new JsonMerge(
                instance.source(),
                instance.children().get(0),
                randomValueOtherThan(instance.children().get(1), AbstractExpressionSerializationTests::randomChild)
            );
    }
}
