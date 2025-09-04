/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.rules;

import org.elasticsearch.gradle.internal.DependencyContext;
import org.gradle.api.artifacts.ComponentMetadataContext;
import org.gradle.api.artifacts.ComponentMetadataRule;
import org.gradle.api.artifacts.VariantMetadata;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.internal.artifacts.repositories.resolver.VariantMetadataAdapter;

//@CacheableRule
public abstract class ExcludeTransitivesRule implements ComponentMetadataRule {


    @Override
    public void execute(ComponentMetadataContext context) {
        context.getDetails().allVariants(variant -> {
            variant.withDependencies(dependencies -> {
                // Exclude all transitive dependencies
                if(dependencies.isEmpty()) {
                    System.out.println(context.getDetails().getId().getGroup() + ":" + context.getDetails().getId().getName());
                }
                dependencies.clear();
//                dependencies.removeIf(p -> {
//                    System.out.println("p.getName() = " + p.getName());
//                    return true;
//                });
            });
        });
    }
//
//    private boolean isCodeQuality(VariantMetadata variant) {
//        // System.out.println("CodeQualityRule#context#variantName = " + ((VariantMetadataAdapter)variant).variantName);
//        System.out.println("ExcludeTransitivesRule#context#isCodeQuality = " + ((VariantMetadataAdapter) variant).toString());
//        try {
//            java.lang.reflect.Field field = VariantMetadataAdapter.class.getDeclaredField("variantName");
//            field.setAccessible(true);
//            Object variantName = field.get(variant);
//            System.out.println("ExcludeTransitivesRule#context#variantName = " + variantName);
//        } catch (Exception e) {
//            System.out.println("Failed to access variantName: " + e.getMessage());
//        }
//        DependencyContext attribute = variant.getAttributes().getAttribute(DependencyContext.CONTEXT_ATTRIBUTE);
//        return attribute != null && DependencyContext.CODE_QUALITY.equals(attribute.getName());
//    }

}
