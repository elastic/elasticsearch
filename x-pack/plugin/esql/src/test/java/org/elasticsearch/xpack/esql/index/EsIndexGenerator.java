/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.type.EsFieldTests;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Tuple.tuple;

public class EsIndexGenerator {

    public static EsIndex esIndex(String name) {
        return new EsIndex(name, Map.of(), Map.of(), Set.of());
    }

    public static EsIndex esIndex(String name, Map<String, EsField> mapping) {
        return new EsIndex(name, mapping, Map.of(), Set.of());
    }

    public static EsIndex esIndex(String name, Map<String, EsField> mapping, Map<String, IndexMode> indexNameWithModes) {
        return new EsIndex(name, mapping, indexNameWithModes, Set.of());
    }

    public static EsIndex randomEsIndex() {
        return new EsIndex(ESTestCase.randomIdentifier(), randomMapping(), randomIndexNameWithModes(), Set.of());
    }

    public static Map<String, EsField> randomMapping() {
        int size = ESTestCase.between(0, 10);
        Map<String, EsField> result = new HashMap<>(size);
        while (result.size() < size) {
            result.put(ESTestCase.randomIdentifier(), EsFieldTests.randomAnyEsField(1));
        }
        return result;
    }

    public static Map<String, IndexMode> randomIndexNameWithModes() {
        return ESTestCase.randomMap(0, 10, () -> tuple(ESTestCase.randomIdentifier(), ESTestCase.randomFrom(IndexMode.values())));
    }
}
