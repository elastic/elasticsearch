/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

import static org.hamcrest.Matchers.containsString;

public class TikaServerBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    private final TikaServerBootstrapCheck check = new TikaServerBootstrapCheck();

    public void testAlwaysEnforce() {
        assertTrue(check.alwaysEnforce());
    }

    public void testNonStatelessNodePassesWithoutUrl() {
        // stateless.enabled is false by default — check must succeed regardless of URL
        BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(Settings.EMPTY, null));
        assertTrue(result.isSuccess());
    }

    public void testNonStatelessNodePassesWithUrl() {
        Settings settings = Settings.builder().put(IngestAttachmentPlugin.TIKA_SERVER_URL_SETTING.getKey(), "http://tika:9998").build();
        BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isSuccess());
    }

    public void testStatelessNodeFailsWithoutUrl() {
        Settings settings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isFailure());
        assertThat(result.getMessage(), containsString(IngestAttachmentPlugin.TIKA_SERVER_URL_SETTING.getKey()));
    }

    public void testStatelessNodePassesWithUrl() {
        Settings settings = Settings.builder()
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .put(IngestAttachmentPlugin.TIKA_SERVER_URL_SETTING.getKey(), "http://tika:9998")
            .build();
        BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isSuccess());
    }

    public void testEscapeHatchAllowsStatelessNodeWithoutUrl() {
        // Internal override: stateless node with no URL passes when the escape hatch is set
        Settings settings = Settings.builder()
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .put(IngestAttachmentPlugin.ALLOW_LOCAL_IN_STATELESS_SETTING.getKey(), true)
            .build();
        BootstrapCheck.BootstrapCheckResult result = check.check(createTestContext(settings, null));
        assertTrue(result.isSuccess());
    }
}
