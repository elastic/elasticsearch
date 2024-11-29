/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

/**
 * Internal policy type (not-parseable -- not available to plugins).
 * Simple yes/no (is entitled/is not entitled) for a specific Entitlement type.
 * @param type the Entitlement type that is allowed.
 */
public record FlagEntitlement(FlagEntitlementType type) implements Entitlement {

}
