/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DesiredNodeTests extends ESTestCase {
    public void testExternalIdFallbacksToNodeName() {
        final String nodeName = randomAlphaOfLength(10);
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build();

        DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
        assertThat(desiredNode.externalId(), is(notNullValue()));
        assertThat(desiredNode.externalId(), is(equalTo(nodeName)));
    }
}
