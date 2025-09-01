/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.rules;

import org.gradle.api.artifacts.CacheableRule;
import org.gradle.api.artifacts.ComponentMetadataContext;
import org.gradle.api.artifacts.ComponentMetadataRule;


@CacheableRule
public class TransitiveFalseByDefaultRule implements ComponentMetadataRule {
    @Override
    public void execute(ComponentMetadataContext componentMetadataContext) {
        if (componentMetadataContext.getDetails().getId().getGroup().contains("elasticsearch") == false) {
            // for spotless we require transitive dependencies
            if (isSpotless(componentMetadataContext)) {
                return;
            }
            componentMetadataContext.getDetails().allVariants(variants -> {
                variants.withDependencies(dependencies -> {
                    dependencies.removeIf(directDependencyMetadata -> true);
                });
            });
        }
    }

    private static boolean isSpotless(ComponentMetadataContext componentMetadataContext) {
        if (componentMetadataContext.getDetails().getId().getGroup().equals("com.google.googlejavaformat")
            && componentMetadataContext.getDetails().getId().getName().equals("google-java-format")) {
            return true;
        }
        return false;
    }
}
