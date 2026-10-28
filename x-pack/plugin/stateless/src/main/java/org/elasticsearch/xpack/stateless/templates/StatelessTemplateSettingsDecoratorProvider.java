/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.templates;

import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.metadata.TemplateDecoratorProvider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

/**
 * Stateless plugin's implementation of {@link TemplateDecoratorProvider}.
 * This implementation filters out ILM (Index Lifecycle Management) settings from default xpack templates.
 * These settings are unavailable in stateless mode.
 */
public class StatelessTemplateSettingsDecoratorProvider implements TemplateDecoratorProvider {

    public StatelessTemplateSettingsDecoratorProvider() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessTemplateSettingsDecoratorProvider(StatelessPlugin plugin) {}

    @Override
    public Template.TemplateDecorator get() {
        return new StatelessTemplateSettingsDecorator();
    }

    private class StatelessTemplateSettingsDecorator implements Template.TemplateDecorator {

        @Override
        public Settings decorate(String template, Settings settings) {
            if (settings == null) {
                return null;
            }
            // filter out ILM settings in stateless
            return settings.filter(key -> key.startsWith("index.lifecycle.") == false);
        }
    }
}
