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

import java.util.List;

import javax.inject.Inject;

/**
 * A Gradle component metadata rule that excludes transitive dependencies of a specific groups.
 *
 * <p>The rule operates on all variants of a component.</p>
 *
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * dependencies {
 *     components {
 *        withModule("org.apache.logging.log4j:log4j-api", ExcludeByGroup) {
 *            params(["biz.aQute.bnd", "org.osgi"])
 *        }
 *     }
 * }
 * }</pre>
 *
 */
@CacheableRule
public class ExcludeByGroup implements ComponentMetadataRule {

    private final List<String> groupIds;

    @Inject
    public ExcludeByGroup(List<String> groupIds) {
        this.groupIds = groupIds;
    }

    @Override
    public void execute(ComponentMetadataContext context) {
        context.getDetails()
            .allVariants(v -> v.withDependencies(dependencies -> dependencies.removeIf(dep -> groupIds.contains(dep.getGroup()))));
    }
}
