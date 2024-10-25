/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyUtils;

import java.util.Collections;
import java.util.Map;

/**
 * Describes an index lifecycle policy to be loaded from a resource file for use with an {@link IndexTemplateRegistry}.
 */
public class JsonLifecyclePolicyConfig extends LifecyclePolicyConfig {

    public JsonLifecyclePolicyConfig(String policyName, String fileName) {
        this(policyName, fileName, Collections.emptyMap());
    }

    public JsonLifecyclePolicyConfig(String policyName, String fileName, Map<String, String> templateVariables) {
        super(policyName, fileName, templateVariables);
    }

    @Override
    public XContentType getXContentType() {
        return XContentType.JSON;
    }

    @Override
    public LifecyclePolicy load(NamedXContentRegistry xContentRegistry) {
        return LifecyclePolicyUtils.loadPolicy(
            getPolicyName(),
            getFileName(),
            this.getTemplateVariables(),
            xContentRegistry,
            getXContentType()
        );
    }
}
