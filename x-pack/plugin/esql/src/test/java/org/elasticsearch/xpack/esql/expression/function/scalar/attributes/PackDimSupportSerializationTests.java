/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.util.List;

public class PackDimSupportSerializationTests extends AbstractExpressionSerializationTests<PackDimSupport> {
    @Override
    protected PackDimSupport createTestInstance() {
        var source = randomSource();
        var packed = new ReferenceAttribute(source, null, "packed", DataType.PACK_DIM);
        var dim = new ReferenceAttribute(source, null, randomAlphaOfLength(8), DataType.KEYWORD);
        return switch (randomFrom(PackDimSupport.Operation.values())) {
            case PACK -> PackDimSupport.pack(source, List.of(dim));
            case GET -> PackDimSupport.get(packed, dim);
            case SET -> PackDimSupport.set(packed, new Alias(source, dim.name(), Literal.keyword(source, randomAlphaOfLength(8))));
            case UNSET -> PackDimSupport.unset(packed, List.of(dim));
            case MAP_FROM_DIMENSIONS -> PackDimSupport.mapFromDimensions(packed, PackDimSupport.unset(packed, List.of(dim)));
            case SET_FROM_DIMENSIONS -> PackDimSupport.setFromDimensions(
                packed,
                new Alias(source, dim.name(), Literal.keyword(source, randomAlphaOfLength(8)))
            );
        };
    }

    @Override
    protected PackDimSupport mutateInstance(PackDimSupport instance) {
        return instance.operation() == PackDimSupport.Operation.UNSET
            ? PackDimSupport.pack(instance.source(), List.of())
            : PackDimSupport.unset(new ReferenceAttribute(instance.source(), null, "other", DataType.PACK_DIM), List.of());
    }

    public void testOldTransportRejectedBeforeWritingPayload() throws Exception {
        try (var output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.minimumCompatible());
            expectThrows(java.io.IOException.class, () -> PackDimSupport.pack(Source.EMPTY, List.of()).writeTo(output));
            assertEquals(0, output.size());
        }
    }
}
