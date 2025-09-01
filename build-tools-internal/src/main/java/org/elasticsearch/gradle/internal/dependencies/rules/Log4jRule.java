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
public abstract class Log4jRule implements ComponentMetadataRule {
    @Override
    public void execute(ComponentMetadataContext context) {
        context.getDetails().withVariant("apiElements", variantMetadata -> {
            variantMetadata.withDependencies(
                deps -> {
                    deps.add("org.jspecify:jspecify:1.0.0");
                    deps.add("com.github.spotbugs:spotbugs-annotations:4.9.3");
                    deps.add("com.google.errorprone:error_prone_annotations:2.38.0");
                }
            );
        });
    }
}
