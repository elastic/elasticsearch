/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfigurationTests;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateTests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsString;

public class GetComposableIndexTemplateResponseTests extends AbstractWireSerializingTestCase<GetComposableIndexTemplateAction.Response> {
    @Override
    protected Writeable.Reader<GetComposableIndexTemplateAction.Response> instanceReader() {
        return GetComposableIndexTemplateAction.Response::new;
    }

    @Override
    protected GetComposableIndexTemplateAction.Response createTestInstance() {
        RolloverConfiguration rolloverConfiguration = randomBoolean() ? null : RolloverConfigurationTests.randomRolloverConditions();
        if (randomBoolean()) {
            return new GetComposableIndexTemplateAction.Response(Map.of(), rolloverConfiguration);
        }
        Map<String, ComposableIndexTemplate> templates = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            templates.put(randomAlphaOfLength(4), ComposableIndexTemplateTests.randomInstance());
        }
        return new GetComposableIndexTemplateAction.Response(templates, rolloverConfiguration);
    }

    @Override
    protected GetComposableIndexTemplateAction.Response mutateInstance(GetComposableIndexTemplateAction.Response instance) {
        var rolloverConfiguration = instance.getRolloverConfiguration();
        var templates = instance.indexTemplates();
        switch (randomInt(1)) {
            case 0 -> rolloverConfiguration = randomBoolean() || rolloverConfiguration == null
                ? randomValueOtherThan(rolloverConfiguration, RolloverConfigurationTests::randomRolloverConditions)
                : null;
            case 1 -> {
                var updatedTemplates = new HashMap<String, ComposableIndexTemplate>();
                for (String name : templates.keySet()) {
                    if (randomBoolean()) {
                        updatedTemplates.put(name, templates.get(name));
                    }
                }
                updatedTemplates.put(randomAlphaOfLength(4), ComposableIndexTemplateTests.randomInstance());
                templates = updatedTemplates;
            }
        }
        return new GetComposableIndexTemplateAction.Response(templates, rolloverConfiguration);
    }

    public void testXContentSerialization() throws IOException {
        String name = randomAlphaOfLength(10);
        var response = new GetComposableIndexTemplateAction.Response(
            Map.of(name, ComposableIndexTemplateTests.randomInstance()),
            randomBoolean() ? null : RolloverConfigurationTests.randomRolloverConditions()
        );

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            Iterator<? extends ToXContent> chunks = response.toXContentChunked(EMPTY_PARAMS);
            while (chunks.hasNext()) {
                chunks.next().toXContent(builder, EMPTY_PARAMS);
            }
            String serialized = Strings.toString(builder);
            assertThat(serialized, containsString("index_templates"));
            assertThat(serialized, containsString(name));
            for (var indexTemplateName : response.indexTemplates().keySet()) {
                assertThat(serialized, containsString(indexTemplateName));
            }
        }
    }

    /**
     * Each index template must be serialized as its own chunk so that the peak heap needed to render the response is bounded by a single
     * template rather than the whole set. The response also emits the enclosing object and array open/close markers, so the expected chunk
     * count is the number of templates plus four.
     */
    public void testChunking() {
        int numberOfTemplates = randomIntBetween(0, 100);
        Map<String, ComposableIndexTemplate> templates = new HashMap<>();
        for (int i = 0; i < numberOfTemplates; i++) {
            templates.put(randomAlphaOfLength(10) + i, ComposableIndexTemplateTests.randomInstance());
        }
        var response = new GetComposableIndexTemplateAction.Response(
            templates,
            randomBoolean() ? null : RolloverConfigurationTests.randomRolloverConditions()
        );
        AbstractChunkedSerializingTestCase.assertChunkCount(response, r -> r.indexTemplates().size() + 4);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(IndicesModule.getNamedWriteables());
    }
}
