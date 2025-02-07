/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.entitlements;

import org.elasticsearch.entitlement.runtime.policy.PolicyValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class FilesEntitlementTests extends ESTestCase {

    public void testEmptyBuild() {
        PolicyValidationException pve = expectThrows(PolicyValidationException.class, () -> FilesEntitlement.build(List.of()));
        assertEquals(pve.getMessage(), "must specify at least one path");
        pve = expectThrows(PolicyValidationException.class, () -> FilesEntitlement.build(null));
        assertEquals(pve.getMessage(), "must specify at least one path");
    }
}
