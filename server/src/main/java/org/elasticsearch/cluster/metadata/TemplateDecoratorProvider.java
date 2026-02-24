/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * SPI service interface to modify templates in Stateless mode.
 *
 * Note: currently decorators are not applied in any particular order.
 */
public interface TemplateDecoratorProvider extends Supplier<Template.TemplateDecorator> {
    InstanceHolder TEMPLATE_DECORATOR = new InstanceHolder();

    /**
     * Get the singleton {@link Template.TemplateDecorator} instance.
     *
     * Initialization is done during node construction and must have happened before.
     */
    static Template.TemplateDecorator getInstance() {
        Template.TemplateDecorator decorator = TEMPLATE_DECORATOR.instance.get();
        if (decorator == null) {
            var illegalStateException = new IllegalStateException("TemplateDecoratorProvider not initialized");
            LogManager.getLogger(TemplateDecoratorProvider.class)
                .warn("Attempted to access TemplateDecorator instance before initialization", illegalStateException);
            throw illegalStateException;
        }
        return decorator;
    }

    /**
     * Initialize the singleton {@link Template.TemplateDecorator} instance from the given list of providers.
     *
     * This method must be called only once during node construction loading available providers via SPI.
     */
    static void initOnce(List<? extends TemplateDecoratorProvider> providers) {
        Template.TemplateDecorator decorator = switch (providers.size()) {
            case 0 -> Template.TemplateDecorator.DEFAULT;
            case 1 -> requireNonNull(providers.get(0).get());
            default -> new Template.TemplateDecorator() {
                final Template.TemplateDecorator[] decorators = providers.stream()
                    .map(p -> requireNonNull(p.get()))
                    .toArray(Template.TemplateDecorator[]::new);

                @Override
                public Settings decorate(String template, Settings settings) {
                    for (Template.TemplateDecorator decorator : decorators) {
                        settings = decorator.decorate(template, settings);
                    }
                    return settings;
                }

                @Override
                public DataStreamLifecycle.Template decorate(String template, DataStreamLifecycle.Template lifecycle) {
                    for (Template.TemplateDecorator decorator : decorators) {
                        lifecycle = decorator.decorate(template, lifecycle);
                    }
                    return lifecycle;
                }
            };
        };
        if (TEMPLATE_DECORATOR.instance.compareAndSet(null, decorator) == false) {
            LogManager.getLogger(TemplateDecoratorProvider.class).warn("TemplateDecoratorProvider already initialized");
        }
    }

    class InstanceHolder {
        private final AtomicReference<Template.TemplateDecorator> instance = new AtomicReference<>();

        Template.TemplateDecorator getAndSet(Template.TemplateDecorator decorator) {
            return instance.getAndSet(decorator);
        }
    }
}
