/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PlanTimeProfileTests extends ESTestCase {

    public void testReductionNanosUsesReductionTime() throws Exception {
        PlanTimeProfile profile = new PlanTimeProfile(1L, 2L, 3L);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            profile.toXContent(builder, null);
            builder.endObject();
            assertThat(Strings.toString(builder), equalTo("""
                {"logical_optimization_nanos":1,"physical_optimization_nanos":2,"reduction_nanos":3}"""));
        }
    }

    public void testProfileSchemas() {
        Map<?, ?> timeProperties = (Map<?, ?>) PlanTimeProfile.PROFILE_SCHEMA.schema().get("properties");
        assertEquals(
            List.of("logical_optimization_nanos", "physical_optimization_nanos", "reduction_nanos"),
            new ArrayList<>(timeProperties.keySet())
        );
        assertEquals(List.of(), PlanTimeProfile.PROFILE_SCHEMA.schema().get("required"));

        Map<?, ?> planProperties = (Map<?, ?>) PlanProfile.PROFILE_SCHEMA.schema().get("properties");
        assertEquals(
            List.of(
                "description",
                "cluster_name",
                "node_name",
                "plan",
                "logical_plan",
                "logical_optimization_nanos",
                "physical_optimization_nanos",
                "reduction_nanos"
            ),
            new ArrayList<>(planProperties.keySet())
        );
        assertEquals(List.of("description", "cluster_name", "node_name", "plan"), PlanProfile.PROFILE_SCHEMA.schema().get("required"));
    }
}
