/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfigurationTests;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComponentTemplateTests;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.ResettableValue;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomAliases;
import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomMappings;
import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomSettings;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsString;

public class GetComponentTemplateResponseTests extends ESTestCase {

    public void testXContentSerializationWithRolloverAndEffectiveRetention() throws IOException {
        Settings settings = null;
        CompressedXContent mappings = null;
        Map<String, AliasMetadata> aliases = null;
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;
        ResettableValue<DataStreamOptions.Template> dataStreamOptions = ResettableValue.undefined();
        if (randomBoolean()) {
            settings = randomSettings();
        }
        if (randomBoolean()) {
            mappings = randomMappings();
        }
        if (randomBoolean()) {
            aliases = randomAliases();
        }
        if (randomBoolean()) {
            dataStreamOptions = ComponentTemplateTests.randomDataStreamOptionsTemplate();
        }

        var template = new ComponentTemplate(
            new Template(settings, mappings, aliases, lifecycle, dataStreamOptions),
            randomBoolean() ? null : randomNonNegativeLong(),
            null,
            false,
            null,
            null
        );
        var rolloverConfiguration = RolloverConfigurationTests.randomRolloverConfiguration();
        var response = new GetComponentTemplateAction.Response(Map.of(randomAlphaOfLength(10), template), rolloverConfiguration);

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            Iterator<? extends ToXContent> chunks = response.toXContentChunked(EMPTY_PARAMS);
            while (chunks.hasNext()) {
                chunks.next().toXContent(builder, EMPTY_PARAMS);
            }
            String serialized = Strings.toString(builder);
            assertThat(serialized, containsString("rollover"));
            for (String label : rolloverConfiguration.resolveRolloverConditions(
                lifecycle.toDataStreamLifecycle().getEffectiveDataRetention(null, randomBoolean())
            ).getConditions().keySet()) {
                assertThat(serialized, containsString(label));
            }
        }
    }

    /**
     * Each component template must be serialized as its own chunk so that the peak heap needed to render the response is bounded by a
     * single template rather than the whole set. The response also emits the enclosing object and array open/close markers, so the
     * expected chunk count is the number of templates plus four.
     */
    public void testChunking() {
        int numberOfTemplates = randomIntBetween(0, 100);
        Map<String, ComponentTemplate> templates = new HashMap<>();
        for (int i = 0; i < numberOfTemplates; i++) {
            templates.put(randomAlphaOfLength(10) + i, ComponentTemplateTests.randomInstance());
        }
        var response = new GetComponentTemplateAction.Response(
            templates,
            randomBoolean() ? null : RolloverConfigurationTests.randomRolloverConfiguration()
        );
        AbstractChunkedSerializingTestCase.assertChunkCount(response, r -> r.getComponentTemplates().size() + 4);
    }
}
