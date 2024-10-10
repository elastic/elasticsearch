/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.reservedstate;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;

import java.util.Collections;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/**
 * Tests that the ReservedRoleMappingAction does validation, can add and remove role mappings
 */
public class ReservedRoleMappingActionTests extends ESTestCase {

    private TransformState processJSON(ReservedRoleMappingAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            var content = action.fromXContent(parser);
            return action.transform(content, prevState);
        }
    }

    public void testValidation() {
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedRoleMappingAction action = new ReservedRoleMappingAction();
        String badPolicyJSON = """
            {
               "everyone_kibana": {
                  "enabled": true,
                  "roles": [ "inter_planetary_role" ],
                  "rules": { "field": { "username": "*" } },
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                     "_reserved": true
                  }
               },
               "everyone_fleet": {
                  "enabled": true,
                  "roles": [ "fleet_user" ],
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7"
                  }
               }
            }""";
        assertEquals(
            "failed to parse role-mapping [everyone_fleet]. missing field [rules]",
            expectThrows(ParsingException.class, () -> processJSON(action, prevState, badPolicyJSON)).getMessage()
        );
    }

    public void testAddRemoveRoleMapping() throws Exception {
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedRoleMappingAction action = new ReservedRoleMappingAction();
        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String json = """
            {
               "everyone_kibana": {
                  "enabled": true,
                  "roles": [ "kibana_user" ],
                  "rules": { "field": { "username": "*" } },
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                     "_reserved": true
                  }
               },
               "everyone_fleet": {
                  "enabled": true,
                  "roles": [ "fleet_user" ],
                  "rules": { "field": { "username": "*" } },
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7",
                     "_reserved": true
                  }
               }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, json);
        assertThat(updatedState.keys(), containsInAnyOrder("everyone_kibana", "everyone_fleet"));

        updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
    }
}
