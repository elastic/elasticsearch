/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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

    private final boolean enabled;

    public StatelessTemplateSettingsDecoratorProvider() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessTemplateSettingsDecoratorProvider(StatelessPlugin plugin) {
        this.enabled = plugin.isEnabled();
    }

    @Override
    public Template.TemplateDecorator get() {
        return enabled ? new StatelessTemplateSettingsDecorator() : Template.TemplateDecorator.DEFAULT;
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
