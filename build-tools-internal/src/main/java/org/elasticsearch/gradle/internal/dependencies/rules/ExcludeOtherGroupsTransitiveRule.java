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
 * A Gradle component metadata rule that excludes transitive dependencies with different group IDs
 * than the parent component.
 *
 * <p>This rule helps prevent dependency conflicts and reduces the dependency tree size by ensuring
 * that only dependencies from the same Maven group ID are included transitively. This is particularly
 * useful for large projects where different modules should not pull in external dependencies from
 * other organizations or groups.</p>
 *
 * <p>The rule operates on all variants of a component and removes any transitive dependency whose
 * group ID differs from the component's own group ID.</p>
 *
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * dependencies {
 *     components {
 *        withModule("com.azure:azure-core", ExcludeOtherGroupsTransitiveRule)
 *     }
 * }
 * }</pre>
 *
 * <h3>Example Behavior:</h3>
 * <p>If component {@code com.example:my-lib:1.0} has transitive dependencies:</p>
 * <ul>
 *   <li>{@code com.example:another-lib:1.0} - KEPT (same group)</li>
 *   <li>{@code org.apache.commons:commons-lang3:3.12.0} - REMOVED (different group)</li>
 *   <li>{@code com.example:utils:1.5} - KEPT (same group)</li>
 * </ul>
 *
 */
@CacheableRule
public abstract class ExcludeOtherGroupsTransitiveRule implements ComponentMetadataRule {

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
