/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class PolicyManager {

    protected final Policy defaultPolicy;
    protected final Map<String, Policy> pluginPolicies;

    public PolicyManager(Policy defaultPolicy, Map<String, Policy> pluginPolicies) {
        this.defaultPolicy = Objects.requireNonNull(defaultPolicy);
        this.pluginPolicies = Collections.unmodifiableMap(Objects.requireNonNull(pluginPolicies));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PolicyManager that = (PolicyManager) o;
        return Objects.equals(defaultPolicy, that.defaultPolicy) && Objects.equals(pluginPolicies, that.pluginPolicies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultPolicy, pluginPolicies);
    }

    @Override
    public String toString() {
        return "PolicyManager{" + "defaultPolicy=" + defaultPolicy + ", pluginPolicies=" + pluginPolicies + '}';
    }
}
