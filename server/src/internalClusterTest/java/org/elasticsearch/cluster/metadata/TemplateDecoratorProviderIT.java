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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Collection;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.core.TimeValue.timeValueDays;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TemplateDecoratorProviderIT extends ESIntegTestCase {

    private static final String TEMPLATE = """
        {
            "settings": {
                "index.number_of_shards": 1,
                "index.number_of_replicas": 1
            },
            "lifecycle": {
                "data_retention": "7d",
                "downsampling": {
                    "after": "1d",
                    "fixed_interval": "1h"
                }
            }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PluginWithDecorator.class);
    }

    public void testTemplateDecoratorProvider() throws Exception {
        internalCluster().startNode();

        Template.TemplateDecorator decorator = TemplateDecoratorProvider.getInstance();
        assertThat(decorator, instanceOf(TestTemplateDecorator.class));

        // everything but index.number_of_shards dropped
        Settings expectedSettings = Settings.builder().put("index.number_of_shards", 1).build();
        // data_retention overwritten to 3d
        DataStreamLifecycle.Template expectedLifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .dataRetention(timeValueDays(3))
            .downsamplingRounds(List.of(new DataStreamLifecycle.DownsamplingRound(timeValueDays(1), DateHistogramInterval.HOUR)))
            .buildTemplate();

        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, TEMPLATE.getBytes(UTF_8))) {
            var template = Template.parse(parser, "template", decorator);
            assertThat(template.settings(), equalTo(expectedSettings));
            assertThat(template.lifecycle(), equalTo(expectedLifecycle));
        }
    }

    // decorator that keeps only index.number_of_shards and sets data_retention to 3 days
    private static class TestTemplateDecorator implements Template.TemplateDecorator {
        @Override
        public Settings decorate(String template, Settings settings) {
            return settings.filter("index.number_of_shards"::equals);
        }

        @Override
        public DataStreamLifecycle.Template decorate(String template, DataStreamLifecycle.Template lifecycle) {
            return DataStreamLifecycle.builder(lifecycle).dataRetention(timeValueDays(3)).buildTemplate();
        }
    }

    public static class PluginWithDecorator extends Plugin {}

    public static class TestTemplateDecoratorProvider implements TemplateDecoratorProvider {

        // making sure the provider is exclusive to this test / plugin
        public TestTemplateDecoratorProvider(PluginWithDecorator plugin) {}

        @Override
        public Template.TemplateDecorator get() {
            return new TestTemplateDecorator();
        }
    }
}
