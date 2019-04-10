package org.elasticsearch.xpack.core.template;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyUtils;

public class LifecyclePolicyConfig {

    private final String policyName;
    private final String fileName;

    public LifecyclePolicyConfig(String policyName, String fileName) {
        this.policyName = policyName;
        this.fileName = fileName;
    }

    public String getPolicyName() {
        return policyName;
    }

    public String getFileName() {
        return fileName;
    }

    public LifecyclePolicy load(NamedXContentRegistry xContentRegistry) {
        return LifecyclePolicyUtils.loadPolicy(policyName, fileName, xContentRegistry);
    }
}
