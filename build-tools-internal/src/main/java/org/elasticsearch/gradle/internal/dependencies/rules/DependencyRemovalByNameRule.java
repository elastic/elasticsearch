/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.rules;

import org.gradle.api.artifacts.ComponentMetadataContext;
import org.gradle.api.artifacts.ComponentMetadataRule;

import java.util.List;

public class DependencyRemovalByNameRule implements ComponentMetadataRule {

    private final List<String> modulesToRemove;

    public DependencyRemovalByNameRule(String... modulesToRemove) {
        this.modulesToRemove = List.of(modulesToRemove);
    }

    @Override
    public void execute(ComponentMetadataContext context) {
        context.getDetails().allVariants(variants -> {
            variants.withDependencies(dependencies -> {
                dependencies.removeIf(metadata -> modulesToRemove.contains(metadata.getName()));
            });
        });
    }
}
