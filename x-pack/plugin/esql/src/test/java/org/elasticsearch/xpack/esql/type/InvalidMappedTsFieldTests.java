/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedTsField;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class InvalidMappedTsFieldTests extends AbstractEsFieldTypeTests<InvalidMappedTsField> {

    static InvalidMappedTsField randomInvalidMappedTsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        String errorMessage = randomAlphaOfLength(20);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        return new InvalidMappedTsField(name, errorMessage, new TreeMap<>(properties));
    }

    @Override
    protected InvalidMappedTsField createTestInstance() {
        return randomInvalidMappedTsField(4);
    }

    @Override
    protected InvalidMappedTsField copyInstance(InvalidMappedTsField instance, TransportVersion version) {
        // writeContent throws UnsupportedOperationException; copy directly without going through the wire.
        return new InvalidMappedTsField(instance.getName(), instance.errorMessage(), new TreeMap<>(instance.getProperties()));
    }

    @Override
    protected InvalidMappedTsField mutateInstance(InvalidMappedTsField instance) {
        String name = instance.getName();
        String errorMessage = instance.errorMessage();
        Map<String, EsField> properties = instance.getProperties();
        switch (between(0, 2)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> errorMessage = randomValueOtherThan(errorMessage, () -> randomAlphaOfLength(20));
            case 2 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            default -> throw new IllegalArgumentException();
        }
        return new InvalidMappedTsField(name, errorMessage, new HashMap<>(properties));
    }
}
