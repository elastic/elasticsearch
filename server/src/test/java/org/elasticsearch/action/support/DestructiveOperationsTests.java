/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Set;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DestructiveOperationsTests extends ESTestCase {

    private DestructiveOperations destructiveOperations;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings nodeSettings = Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), "true").build();
        destructiveOperations = new DestructiveOperations(
            nodeSettings,
            new ClusterSettings(nodeSettings, Set.of(DestructiveOperations.REQUIRES_NAME_SETTING))
        );
    }

    public void testDestructive() {
        {
            // requests that might resolve to all indices
            assertFailsDestructive(null);
            assertFailsDestructive(new String[] {});
            assertFailsDestructive(new String[] { "_all" });
            assertFailsDestructive(new String[] { "*" });
        }
        {
            // various wildcards
            assertFailsDestructive(new String[] { "-*" });
            assertFailsDestructive(new String[] { "index*" });
            assertFailsDestructive(new String[] { "index", "*" });
            assertFailsDestructive(new String[] { "index", "-*" });
            assertFailsDestructive(new String[] { "index", "test-*-index" });
        }
        {
            // near versions of the "matchNone" pattern
            assertFailsDestructive(new String[] { "-*", "*" });
            assertFailsDestructive(new String[] { "*", "-*", "*" });
        }
    }

    /**
     * Test that non-wildcard expressions or the special "*,-*" don't throw an
     * exception. Since {@link DestructiveOperations#failDestructive(String[])}
     * has no return value, we run the statements without asserting anything
     * about them.
     */
    public void testNonDestructive() {
        {
            // no wildcards
            destructiveOperations.failDestructive(new String[] { "index" });
            destructiveOperations.failDestructive(new String[] { "index", "-index2" });
        }
        {
            // special "matchNone" pattern
            destructiveOperations.failDestructive(new String[] { "*", "-*" });
        }
    }

    /**
     * Test that we get a deprecation warning if we try a wildcard deletion and
     * action.destructive_requires_name is unset.
     */
    public void testDeprecationOfDefaultValue() {
        DestructiveOperations destructiveOperations = new DestructiveOperations(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, Set.of(DestructiveOperations.REQUIRES_NAME_SETTING))
        );
        {
            destructiveOperations.failDestructive(new String[] { "index-*" });
            assertWarnings(DestructiveOperations.DESTRUCTIVE_REQUIRES_NAME_DEPRECATION_WARNING);
        }
        {
            destructiveOperations.failDestructive(new String[] { "index" });
            // no warning
        }
        {
            destructiveOperations.failDestructive(new String[] {});
            assertWarnings(DestructiveOperations.DESTRUCTIVE_REQUIRES_NAME_DEPRECATION_WARNING);
        }
    }

    /**
     * Test that applying settings enables and disables the deprecation warning.
     */
    public void testDeprecationWarningClusterSettingsSync() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(DestructiveOperations.REQUIRES_NAME_SETTING));
        DestructiveOperations destructiveOperations = new DestructiveOperations(Settings.EMPTY, clusterSettings);

        {
            // empty settings gives us a warning
            destructiveOperations.failDestructive(new String[] { "index-*" });
            assertWarnings(DestructiveOperations.DESTRUCTIVE_REQUIRES_NAME_DEPRECATION_WARNING);
        }
        {
            // setting to "true" removes the deprecation warning and fails the operation
            clusterSettings.applySettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), "true").build());

            assertFailsDestructive(new String[] { "index-*" });
        }
        {
            // restoring the empty setting restores the warning
            clusterSettings.applySettings(Settings.EMPTY);

            destructiveOperations.failDestructive(new String[] { "index-*" });
            assertWarnings(DestructiveOperations.DESTRUCTIVE_REQUIRES_NAME_DEPRECATION_WARNING);
        }
        {
            // explicitly set to false: no warning, no failure
            clusterSettings.applySettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), "false").build());

            destructiveOperations.failDestructive(new String[] { "index-*" });
        }
    }

    private void assertFailsDestructive(String[] aliasesOrIndices) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> destructiveOperations.failDestructive(aliasesOrIndices)
        );

        assertThat(e.getMessage(), equalTo("Wildcard expressions or all indices are not allowed"));
    }
}
