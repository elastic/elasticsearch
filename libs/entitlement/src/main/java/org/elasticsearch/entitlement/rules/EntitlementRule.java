/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.rules.function.VarargCall;

/**
 * Represents an entitlement rule for a specific method.
 * <p>
 * An entitlement rule consists of:
 * <ul>
 *   <li>A method key identifying the method to be instrumented</li>
 *   <li>A check method that performs the entitlement validation</li>
 *   <li>A strategy defining what happens when the entitlement check fails</li>
 * </ul>
 *
 * @param methodKey the method identifier for which this rule applies
 * @param checkMethod the function that performs the entitlement check
 * @param strategy the strategy to apply when the entitlement check fails
 */
public record EntitlementRule(MethodKey methodKey, VarargCall<CheckMethod> checkMethod, DeniedEntitlementStrategy strategy) {}
