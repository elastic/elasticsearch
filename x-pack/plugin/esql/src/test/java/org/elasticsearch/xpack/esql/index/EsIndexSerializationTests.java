/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.EsFieldTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EsIndexSerializationTests extends AbstractWireSerializingTestCase<EsIndex> {
    public static EsIndex randomEsIndex() {
        String name = randomAlphaOfLength(5);
        Map<String, EsField> mapping = randomMapping();
        Set<String> concreteIndices = randomConcreteIndices();
        return new EsIndex(name, mapping, concreteIndices);
    }

    private static Map<String, EsField> randomMapping() {
        int size = between(0, 10);
        Map<String, EsField> result = new HashMap<>(size);
        while (result.size() < size) {
            result.put(randomAlphaOfLength(5), EsFieldTests.randomAnyEsField(1));
        }
        return result;
    }

    private static Set<String> randomConcreteIndices() {
        int size = between(0, 10);
        Set<String> result = new HashSet<>(size);
        while (result.size() < size) {
            result.add(randomAlphaOfLength(5));
        }
        return result;
    }

    @Override
    protected Writeable.Reader<EsIndex> instanceReader() {
        return EsIndex::new;
    }

    @Override
    protected EsIndex createTestInstance() {
        return randomEsIndex();
    }

    @Override
    protected EsIndex mutateInstance(EsIndex instance) throws IOException {
        String name = instance.name();
        Map<String, EsField> mapping = randomMapping();
        Set<String> concreteIndices = randomConcreteIndices();
        switch (between(0, 2)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLength(5));
            case 1 -> mapping = randomValueOtherThan(mapping, EsIndexSerializationTests::randomMapping);
            case 2 -> concreteIndices = randomValueOtherThan(concreteIndices, EsIndexSerializationTests::randomConcreteIndices);
            default -> throw new IllegalArgumentException();
        }
        return new EsIndex(name, mapping, concreteIndices);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(EsField.getNamedWriteables());
    }
}
