/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.bridge.Java22EntitlementChecker;

public class Java22ElasticsearchEntitlementChecker extends Java21ElasticsearchEntitlementChecker implements Java22EntitlementChecker {

    public Java22ElasticsearchEntitlementChecker(PolicyChecker policyChecker) {
        super(policyChecker);
    }
}
