/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.rules;

import org.elasticsearch.gradle.internal.precommit.DependencyContext;
import org.gradle.api.artifacts.CacheableRule;
import org.gradle.api.artifacts.ComponentMetadataContext;
import org.gradle.api.artifacts.ComponentMetadataRule;
import org.gradle.api.attributes.Attribute;

@CacheableRule
public class ExcludeTransitivesRule implements ComponentMetadataRule {

    private static Attribute<DependencyContext> DEPENDENCY_CONTEXT_ATTRIBUTE = Attribute.of(DependencyContext.class);

    @Override
    public void execute(ComponentMetadataContext context) {
        if (context.getDetails().getId().getGroup().startsWith("org.elasticsearch") == false) {
            // for code quality dependencies we rely on transitive dependencies
            context.getDetails().allVariants(variants -> {
                DependencyContext attribute = context.getDetails().getAttributes().getAttribute(DEPENDENCY_CONTEXT_ATTRIBUTE);
                System.out.println("attribute = " + attribute);

                variants.withDependencies(dependencies -> { dependencies.removeIf(directDependencyMetadata -> true); });
            });
        }
    }
}
