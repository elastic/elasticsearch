/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.stream.Collectors;

public class EsqlProfileSchemaRegistryTests extends ESTestCase {

    public void testEveryRegisteredOperatorStatusHasASchema() {
        EsqlPlugin plugin = new EsqlPlugin();
        Set<String> registered = plugin.getNamedWriteables()
            .stream()
            .filter(entry -> entry.categoryClass == Operator.Status.class)
            .map(entry -> entry.name)
            .collect(Collectors.toSet());
        Set<String> described = EsqlProfileSchemaRegistry.operatorStatusVariants()
            .stream()
            .map(variant -> variant.name())
            .collect(Collectors.toSet());
        assertEquals(registered, described);
    }

    public void testEveryRegisteredExtraStatusHasASchema() {
        EsqlPlugin plugin = new EsqlPlugin();
        Set<String> registered = plugin.getNamedWriteables()
            .stream()
            .filter(entry -> entry.categoryClass == Operator.Status.ExtraStatus.class)
            .map(entry -> entry.name)
            .collect(Collectors.toSet());
        Set<String> described = EsqlProfileSchemaRegistry.extraStatusVariants()
            .stream()
            .map(variant -> variant.name())
            .collect(Collectors.toSet());
        assertEquals(registered, described);
    }
}
