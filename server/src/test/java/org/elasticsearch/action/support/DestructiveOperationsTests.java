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
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DestructiveOperationsTests extends ESTestCase {

    private DestructiveOperations destructiveOperations;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings nodeSettings = Settings.builder()
            .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), "true")
            .build();
        destructiveOperations = new DestructiveOperations(
            nodeSettings,
            new ClusterSettings(nodeSettings, Set.of(DestructiveOperations.REQUIRES_NAME_SETTING)));
    }

    public void testDestructive() {
        {
            // requests that might resolve to all indices
            assertFailsDestructive(null);
            assertFailsDestructive(new String[]{});
            assertFailsDestructive(new String[]{"_all"});
            assertFailsDestructive(new String[]{"*"});
        }
        {
            // various wildcards
            assertFailsDestructive(new String[] {"-*"});
            assertFailsDestructive(new String[] {"index*"});
            assertFailsDestructive(new String[] {"index", "*"});
            assertFailsDestructive(new String[] {"index", "-*"});
            assertFailsDestructive(new String[] {"index", "test-*-index"});
        }
        {
            // near versions of the "matchNone" pattern
            assertFailsDestructive(new String[]{"-*", "*"});
            assertFailsDestructive(new String[]{"*", "-*", "*"});
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
            destructiveOperations.failDestructive(new String[]{"index"});
            destructiveOperations.failDestructive(new String[]{"index", "-index2"});
        }
        {
            // special "matchNone" pattern
            destructiveOperations.failDestructive(new String[]{"*", "-*"});
        }
    }

    private void assertFailsDestructive(String[] aliasesOrIndices) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> destructiveOperations.failDestructive(aliasesOrIndices));

        assertThat(e.getMessage(), equalTo("Wildcard expressions or all indices are not allowed"));
    }
}
