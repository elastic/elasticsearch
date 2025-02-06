/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

/**
 * Java20 variant of {@link EntitlementChecker} handle holder.
 */
public class Java20EntitlementCheckerHandle {

    public static Java20EntitlementChecker instance() {
        return Holder.instance;
    }

    private static class Holder {
        private static final Java20EntitlementChecker instance = HandleLoader.load(Java20EntitlementChecker.class);
    }

    // no construction
    private Java20EntitlementCheckerHandle() {}
}
