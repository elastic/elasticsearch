/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TemplateDecoratorProviderTests extends ESTestCase {

    @Rule
    public final TestRule templateDecoratorRule = TemplateDecoratorRule.reset();

    public void testGetInstanceThrowsWhenNotInitialized() {
        IllegalStateException e = expectThrows(IllegalStateException.class, TemplateDecoratorProvider::getInstance);
        assertThat(e.getMessage(), equalTo("TemplateDecoratorProvider not initialized"));
    }

    public void testInitOnceWithZeroProviders() {
        TemplateDecoratorProvider.initOnce(List.of());
        var decorator = TemplateDecoratorProvider.getInstance();
        assertThat(decorator, is(Template.TemplateDecorator.DEFAULT));
        assertThat(decorator.decorate("t", Settings.EMPTY), is(Settings.EMPTY));
        assertThat(decorator.decorate("t", DataStreamLifecycle.Template.DATA_DEFAULT), is(DataStreamLifecycle.Template.DATA_DEFAULT));
    }

    public void testInitOnceWithOneProvider() {
        Settings expectedSettings = Settings.builder().put("custom.key", "value").build();

        TemplateDecoratorProvider.initOnce(List.of(() -> new Template.TemplateDecorator() {
            @Override
            public Settings decorate(String template, Settings settings) {
                return Settings.builder().put(settings).put(expectedSettings).build();
            }
        }));
        var decorator = TemplateDecoratorProvider.getInstance();
        assertThat(decorator.decorate("t", Settings.EMPTY), equalTo(expectedSettings));
    }

    public void testInitOnceWithMultipleProvidersChainsSettingsAndLifecycle() {
        var keep7days = DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(7)).buildTemplate();

        var decorator1 = new Template.TemplateDecorator() {
            @Override
            public Settings decorate(String template, Settings settings) {
                return Settings.builder().put(settings).put("first", "1").build();
            }
        };
        var decorator2 = new Template.TemplateDecorator() {
            @Override
            public Settings decorate(String template, Settings settings) {
                return Settings.builder().put(settings).put("second", "2").build();
            }

            @Override
            public DataStreamLifecycle.Template decorate(String template, DataStreamLifecycle.Template lifecycle) {
                return keep7days;
            }
        };
        TemplateDecoratorProvider.initOnce(List.of(() -> decorator1, () -> decorator2));
        var decorator = TemplateDecoratorProvider.getInstance();

        var result = decorator.decorate("t", Settings.EMPTY);
        assertThat(result.get("first"), equalTo("1"));
        assertThat(result.get("second"), equalTo("2"));

        var lifecycle = decorator.decorate("t", DataStreamLifecycle.Template.DATA_DEFAULT);
        assertThat(lifecycle, equalTo(keep7days));
    }

    public void testInitOnceOnly() {
        var decorator1 = Mockito.mock(Template.TemplateDecorator.class);
        var decorator2 = Mockito.mock(Template.TemplateDecorator.class);

        TemplateDecoratorProvider.initOnce(List.of(() -> decorator1));
        assertThat(TemplateDecoratorProvider.getInstance(), is(decorator1));

        // lenient for tests, but won't init again
        TemplateDecoratorProvider.initOnce(List.of(() -> decorator2));
        assertThat(TemplateDecoratorProvider.getInstance(), is(decorator1));
    }

    public void testInitOnceWithProviderReturningNullThrows() {
        TemplateDecoratorProvider nullProvider = () -> null;
        expectThrows(NullPointerException.class, () -> TemplateDecoratorProvider.initOnce(List.of(nullProvider)));
    }
}
