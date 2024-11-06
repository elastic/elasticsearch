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

}
