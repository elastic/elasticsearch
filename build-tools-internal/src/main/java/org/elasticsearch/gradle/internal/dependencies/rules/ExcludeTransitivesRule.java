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
import org.gradle.api.artifacts.VariantMetadata;

@CacheableRule
public class ExcludeTransitivesRule implements ComponentMetadataRule {

    @Override
    public void execute(ComponentMetadataContext context) {

        if (context.getDetails().getId().getGroup().startsWith("org.elasticsearch") == false) {

            // for code quality dependencies we rely on transitive dependencies
            context.getDetails().allVariants(variant -> {
                if(isCodeQuality(variant)) {
                    return;
                }
                variant.withDependencies(dependencies -> { dependencies.removeIf(directDependencyMetadata -> true); });
            });
        }
    }

    private boolean isCodeQuality(VariantMetadata variant) {
        System.out.println("variant.getAttributes() = " + variant.getAttributes());
        DependencyContext attribute = variant.getAttributes().getAttribute(DependencyContext.CONTEXT_ATTRIBUTE);
        System.out.println("attribute = " + attribute);
        return attribute != null && DependencyContext.CODE_QUALITY.equals(attribute.getName());
    }

    private boolean codeQuality(ComponentMetadataContext context) {

        DependencyContext attribute = context.getDetails().getAttributes().getAttribute(DependencyContext.CONTEXT_ATTRIBUTE);
        System.out.println("attribute = " + attribute);
        return attribute != null && DependencyContext.CODE_QUALITY.equals(attribute.getName());
    }
}
