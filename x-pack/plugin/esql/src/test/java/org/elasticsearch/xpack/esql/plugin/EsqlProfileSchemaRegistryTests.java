/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
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

    public void testSequenceWorkFieldsAreRequiredIntegers() {
        var variant = EsqlProfileSchemaRegistry.operatorStatusVariants()
            .stream()
            .filter(item -> item.name().equals("sequence_source"))
            .findFirst()
            .orElseThrow();
        var schema = variant.profileSchema().schema();
        assertTrue(schema.get("properties") instanceof Map<?, ?>);
        assertTrue(schema.get("required") instanceof List<?>);
        var properties = (Map<?, ?>) schema.get("properties");
        var required = (List<?>) schema.get("required");
        for (String field : List.of(
            "candidates_dropped_at_bound",
            "searches_abandoned_at_work_limit",
            "keys_that_hit_bound",
            "search_steps",
            "max_search_steps_per_final_row",
            "condition_evaluations",
            "largest_ring_fill"
        )) {
            assertEquals(Map.of("type", "integer"), properties.get(field));
            assertTrue(required.contains(field));
        }
        assertFalse(properties.containsKey("condition_false_or_null"));
        assertFalse(properties.containsKey("candidates_used_without_report"));
    }
}
