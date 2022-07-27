/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class UpdateDesiredNodesRequestTests extends ESTestCase {
    public void testValidation() {
        final UpdateDesiredNodesRequest updateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            randomBoolean() ? "" : "     ",
            -1,
            randomBoolean() ? Collections.emptyList() : List.of(hotDesiredNode()),
            randomBoolean()
        );
        ActionRequestValidationException exception = updateDesiredNodesRequest.validate();
        assertThat(exception, is(notNullValue()));
        assertThat(exception.getMessage(), containsString("version must be positive"));
        assertThat(exception.getMessage(), containsString("nodes must contain at least one master node"));
        assertThat(exception.getMessage(), containsString("historyID should not be empty"));
    }

    private DesiredNode hotDesiredNode() {
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10))
            .put(NODE_ROLES_SETTING.getKey(), "data_hot")
            .build();

        if (randomBoolean()) {
            return new DesiredNode(settings, randomFloat(), ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
        } else {
            return new DesiredNode(
                settings,
                new DesiredNode.ProcessorsRange(1, randomBoolean() ? null : (float) 1),
                ByteSizeValue.ofGb(1),
                ByteSizeValue.ofGb(1),
                Version.CURRENT
            );
        }
    }
}
