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

//@CacheableRule
public abstract class ExcludeTransitiveOtherGroupsRule implements ComponentMetadataRule {

    @Override
    public void execute(ComponentMetadataContext context) {
        context.getDetails().allVariants(variant -> {
            variant.withDependencies(dependencies -> {
                // Exclude transitive dependencies with a different groupId than the parent
                dependencies.removeIf(dep -> dep.getGroup().equals(context.getDetails().getId().getGroup()) == false);
            });
        });
    }
}
