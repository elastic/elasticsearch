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
import java.util.List;
import java.util.Map;

public class InvalidMappedTsFieldTests extends AbstractEsFieldTypeTests<InvalidMappedTsField> {

    static InvalidMappedTsField randomInvalidMappedTsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        List<String> roles = randomSubsetOf(2, "dimension", "metric");
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        return new InvalidMappedTsField(name, roles.get(0), roles.get(1), new HashMap<>(properties));
    }

    @Override
    protected InvalidMappedTsField createTestInstance() {
        return randomInvalidMappedTsField(4);
    }

    @Override
    protected InvalidMappedTsField copyInstance(InvalidMappedTsField instance, TransportVersion version) {
        // writeContent throws UnsupportedOperationException; copy directly without going through the wire.
        List<String> roles = instance.getRoles();
        return new InvalidMappedTsField(instance.getName(), roles.get(0), roles.get(1), new HashMap<>(instance.getProperties()));
    }

    @Override
    protected InvalidMappedTsField mutateInstance(InvalidMappedTsField instance) {
        String name = instance.getName();
        List<String> roles = instance.getRoles();
        String role1 = roles.get(0);
        String role2 = roles.get(1);
        Map<String, EsField> properties = instance.getProperties();
        switch (between(0, 2)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> {
                // swap roles (produces a different instance)
                String tmp = role1;
                role1 = role2;
                role2 = tmp;
            }
            case 2 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            default -> throw new IllegalArgumentException();
        }
        return new InvalidMappedTsField(name, role1, role2, new HashMap<>(properties));
    }
}
