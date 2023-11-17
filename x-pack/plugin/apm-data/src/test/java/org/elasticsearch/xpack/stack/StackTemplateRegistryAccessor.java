/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.elasticsearch.cluster.metadata.ComponentTemplate;

import java.util.Map;

/**
 * An accessor class for package-private functionalities of {@link StackTemplateRegistry}
 */
public class StackTemplateRegistryAccessor {

    final StackTemplateRegistry registry;

    public StackTemplateRegistryAccessor(StackTemplateRegistry registry) {
        this.registry = registry;
    }

    public Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return registry.getComponentTemplateConfigs();
    }
}
