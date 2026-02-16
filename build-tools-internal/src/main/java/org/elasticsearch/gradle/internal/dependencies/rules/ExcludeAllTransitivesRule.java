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

/**
 * A Gradle component metadata rule that excludes all transitive dependencies.
 *
 * <p>The rule operates on all variants of a component.</p>
 *
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * dependencies {
 *     components {
 *        withModule("com.azure:azure-core-http-netty"", ExcludeAllTransitivesRule)
 *     }
 * }
 * }</pre>
 *
 */
@CacheableRule
public abstract class ExcludeAllTransitivesRule implements ComponentMetadataRule {

    @Override
    public void execute(ComponentMetadataContext context) {
        context.getDetails().allVariants(variant -> {
            variant.withDependencies(dependencies -> {
                // Exclude all transitive dependencies
                dependencies.clear();
            });
        });
    }
}
