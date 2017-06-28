/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.bootstrap;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authc.ContainerSettings;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContainerPasswordBootstrapCheckTests extends ESTestCase {

    private ContainerPasswordBootstrapCheck bootstrapCheck;

    public void testCheckPassesIfNoPassword() {
        bootstrapCheck = new ContainerPasswordBootstrapCheck(new ContainerSettings(false, null));

        assertFalse(bootstrapCheck.check());
    }

    public void testCheckPassesIfPasswordAndInContainer() {
        bootstrapCheck = new ContainerPasswordBootstrapCheck(new ContainerSettings(true, "password".toCharArray()));

        assertFalse(bootstrapCheck.check());
    }

    public void testCheckFailsIfPasswordAndNotContainer() {
        bootstrapCheck = new ContainerPasswordBootstrapCheck(new ContainerSettings(false, "password".toCharArray()));

        assertTrue(bootstrapCheck.check());
    }
}
