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

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.core.template.ResourceUtils.loadResource;

public class YamlLifecyclePolicyConfig extends LifecyclePolicyConfig {
    private final Class<?> clazz;

    public YamlLifecyclePolicyConfig(
        String policyName,
        String fileName,
        Class<?> clazz
    ) {
        super(policyName, fileName, Collections.emptyMap());
        this.clazz = clazz;
    }

    @Override
    public XContentType getXContentType() {
        return XContentType.YAML;
    }

    @Override
    public LifecyclePolicy load(NamedXContentRegistry xContentRegistry) {
        try {
            var rawPolicy = loadResource(this.clazz, getFileName());
            rawPolicy = TemplateUtils.replaceVariables(rawPolicy, this.getTemplateVariables());
            return LifecyclePolicyUtils.parsePolicy(
                rawPolicy,
                getPolicyName(),
                LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY,
                getXContentType()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
