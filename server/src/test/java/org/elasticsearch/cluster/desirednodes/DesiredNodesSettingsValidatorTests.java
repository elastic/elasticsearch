/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;

public class DesiredNodesSettingsValidatorTests extends ESTestCase {
    public void testNodeVersionValidation() {
        final List<DesiredNode> desiredNodes = List.of(randomDesiredNode(Version.CURRENT.previousMajor(), Settings.EMPTY));

        final DesiredNodesSettingsValidator validator = new DesiredNodesSettingsValidator();

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> validator.accept(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed(), not(emptyArray()));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("Illegal node version"));
    }
}
