/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class AbstractEsFieldTypeTests<T extends EsField> extends AbstractWireTestCase<EsField> {
    public static EsField randomAnyEsField(int maxDepth) {
        return switch (between(0, 5)) {
            case 0 -> EsFieldTests.randomEsField(maxDepth);
            case 1 -> DateEsFieldTests.randomDateEsField(maxDepth);
            case 2 -> InvalidMappedFieldTests.randomInvalidMappedField(maxDepth);
            case 3 -> KeywordEsFieldTests.randomKeywordEsField(maxDepth);
            case 4 -> TextEsFieldTests.randomTextEsField(maxDepth);
            case 5 -> UnsupportedEsFieldTests.randomUnsupportedEsField(maxDepth);
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    protected abstract T createTestInstance();

    protected abstract T mutate(T instance);

    @Override
    protected EsField copyInstance(EsField instance, TransportVersion version) throws IOException {
        NamedWriteableRegistry namedWriteableRegistry = getNamedWriteableRegistry();
        try (
            BytesStreamOutput output = new BytesStreamOutput();
            var pso = new PlanStreamOutput(output, new PlanNameRegistry(), EsqlTestUtils.TEST_CFG)
        ) {
            pso.setTransportVersion(version);
            instance.writeTo(pso);
            try (
                StreamInput in1 = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry);
                var psi = new PlanStreamInput(in1, new PlanNameRegistry(), in1.namedWriteableRegistry(), EsqlTestUtils.TEST_CFG)
            ) {
                psi.setTransportVersion(version);
                return EsField.readFrom(psi);
            }
        }
    }

    /**
     * Generate sub-properties.
     * @param maxDepth the maximum number of levels of properties to make
     */
    static Map<String, EsField> randomProperties(int maxDepth) {
        if (maxDepth < 0) {
            throw new IllegalArgumentException("depth must be >= 0");
        }
        if (maxDepth == 0 || randomBoolean()) {
            return Map.of();
        }
        int targetSize = between(1, 5);
        Map<String, EsField> properties = new TreeMap<>();
        while (properties.size() < targetSize) {
            properties.put(randomAlphaOfLength(properties.size() + 1), randomAnyEsField(maxDepth - 1));
        }
        return properties;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final T mutateInstance(EsField instance) throws IOException {
        return mutate((T) instance);
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of());
    }
}
