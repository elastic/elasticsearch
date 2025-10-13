/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.entitlements;

import org.elasticsearch.entitlement.runtime.policy.ExternalEntitlement;

import java.util.List;
import java.util.Set;

/**
 * An Entitlement to allow writing properties such as system properties.
 */
public record WriteSystemPropertiesEntitlement(Set<String> properties) implements Entitlement {

    @ExternalEntitlement(parameterNames = { "properties" }, esModulesOnly = false)
    public WriteSystemPropertiesEntitlement(List<String> properties) {
        this(Set.copyOf(properties));
    }
}
