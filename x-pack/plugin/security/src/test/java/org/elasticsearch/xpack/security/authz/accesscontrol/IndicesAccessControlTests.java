/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Collections;

/**
 * Unit tests for {@link IndicesAccessControl}
 */
public class IndicesAccessControlTests extends ESTestCase {

    public void testEmptyIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        assertTrue(indicesAccessControl.isGranted());
        assertNull(indicesAccessControl.getIndexPermissions(randomAlphaOfLengthBetween(3,20)));
    }
}
