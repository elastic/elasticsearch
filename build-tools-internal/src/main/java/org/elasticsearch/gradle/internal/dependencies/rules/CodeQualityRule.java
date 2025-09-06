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
import org.gradle.api.model.ObjectFactory;

import javax.inject.Inject;

public abstract class CodeQualityRule implements ComponentMetadataRule {

    @Inject
    public ObjectFactory getObjects() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(ComponentMetadataContext context) {
        System.out.println("CodeQualityRule#context = " + context.getDetails().getId());
        context.getDetails().addVariant("codeQuality", "runtime", variant -> {
            System.out.println("CodeQualityRule#context#variant = " + variant);
            // variant.attributes(attrs -> {
            // ystem.out.println("Adding codeQuality variant to " + context.getDetails().getId());
            variant.getAttributes()
                .attribute(
                    DependencyContext.CONTEXT_ATTRIBUTE,
                    getObjects().named(DependencyContext.class, DependencyContext.CODE_QUALITY)
                );
        });
    }
}
