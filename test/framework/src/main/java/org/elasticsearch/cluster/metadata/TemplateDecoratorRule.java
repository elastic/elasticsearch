/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * {@link TestRule} to manage initialization of {@link TemplateDecoratorProvider#TEMPLATE_DECORATOR}.
 */
public class TemplateDecoratorRule implements TestRule {

    private final Template.TemplateDecorator decorator;

    /**
     * Test rule that resets {@link TemplateDecoratorProvider#TEMPLATE_DECORATOR} without re-initializing it.
     */
    public static TemplateDecoratorRule reset() {
        return new TemplateDecoratorRule(null);
    }

    /**
     * Test rule that initializes {@link TemplateDecoratorProvider#TEMPLATE_DECORATOR} with the default provider.
     */
    public static TemplateDecoratorRule initDefault() {
        return new TemplateDecoratorRule(Template.TemplateDecorator.DEFAULT);
    }

    private TemplateDecoratorRule(Template.TemplateDecorator decorator) {
        this.decorator = decorator;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                var previous = TemplateDecoratorProvider.TEMPLATE_DECORATOR.getAndSet(decorator);
                try {
                    base.evaluate();
                } finally {
                    TemplateDecoratorProvider.TEMPLATE_DECORATOR.getAndSet(previous);
                }
            }
        };
    }
}
