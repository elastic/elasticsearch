/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InternalTestRerunPluginTests {

    @Test
    public void testRecognizesStatefulUpgradeTaskNames() {
        // rolling-upgrade and full-cluster-restart BWC test tasks
        assertTrue(InternalTestRerunPlugin.isStatefulUpgradeTask("v9.4.4#bwcTest"));
        assertTrue(InternalTestRerunPlugin.isStatefulUpgradeTask("v8.19.13#bwcTest"));
        assertTrue(InternalTestRerunPlugin.isStatefulUpgradeTask("bcUpgradeTest"));
        assertTrue(InternalTestRerunPlugin.isStatefulUpgradeTask("luceneBwcTest"));
    }

    @Test
    public void testIgnoresOtherTaskNames() {
        assertFalse(InternalTestRerunPlugin.isStatefulUpgradeTask("test"));
        assertFalse(InternalTestRerunPlugin.isStatefulUpgradeTask("javaRestTest"));
        assertFalse(InternalTestRerunPlugin.isStatefulUpgradeTask("v9.4.4#javaRestTest"));
        assertFalse(InternalTestRerunPlugin.isStatefulUpgradeTask("v9.4.4#mixedClusterTest"));
        assertFalse(InternalTestRerunPlugin.isStatefulUpgradeTask("v9.4.4#oneThirdUpgraded"));
        assertFalse(InternalTestRerunPlugin.isStatefulUpgradeTask("v9.4.4#bwcTestExtra"));
    }
}
