/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfigurationTests;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComponentTemplateTests;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionTests;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomAliases;
import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomMappings;
import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomSettings;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class GetComponentTemplateResponseTests extends AbstractWireSerializingTestCase<GetComponentTemplateAction.Response> {
    @Override
    protected Writeable.Reader<GetComponentTemplateAction.Response> instanceReader() {
        return GetComponentTemplateAction.Response::new;
    }

    @Override
    protected GetComponentTemplateAction.Response createTestInstance() {
        return new GetComponentTemplateAction.Response(
            randomBoolean() ? Map.of() : randomTemplates(),
            RolloverConfigurationTests.randomRolloverConditions(),
            DataStreamGlobalRetentionTests.randomGlobalRetention()
        );
    }

    @Override
    protected GetComponentTemplateAction.Response mutateInstance(GetComponentTemplateAction.Response instance) {
        var templates = instance.getComponentTemplates();
        var rolloverConditions = instance.getRolloverConfiguration();
        var globalRetention = instance.getGlobalRetention();
        switch (randomInt(2)) {
            case 0 -> templates = templates == null ? randomTemplates() : null;
            case 1 -> rolloverConditions = randomValueOtherThan(rolloverConditions, RolloverConfigurationTests::randomRolloverConditions);
            case 2 -> globalRetention = randomValueOtherThan(globalRetention, DataStreamGlobalRetentionTests::randomGlobalRetention);
        }
        return new GetComponentTemplateAction.Response(templates, rolloverConditions, globalRetention);
    }

    public void testXContentSerializationWithRolloverAndEffectiveRetention() throws IOException {
        Settings settings = null;
        CompressedXContent mappings = null;
        Map<String, AliasMetadata> aliases = null;
        DataStreamLifecycle lifecycle = new DataStreamLifecycle();
        if (randomBoolean()) {
            settings = randomSettings();
        }
        if (randomBoolean()) {
            mappings = randomMappings();
        }
        if (randomBoolean()) {
            aliases = randomAliases();
        }

        var template = new ComponentTemplate(
            new Template(settings, mappings, aliases, lifecycle),
            randomBoolean() ? null : randomNonNegativeLong(),
            null,
            false
        );
        var globalRetention = DataStreamGlobalRetentionTests.randomGlobalRetention();
        var rolloverConfiguration = RolloverConfigurationTests.randomRolloverConditions();
        var response = new GetComponentTemplateAction.Response(
            Map.of(randomAlphaOfLength(10), template),
            rolloverConfiguration,
            globalRetention
        );

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            response.toXContent(builder, EMPTY_PARAMS);
            String serialized = Strings.toString(builder);
            assertThat(serialized, containsString("rollover"));
            for (String label : rolloverConfiguration.resolveRolloverConditions(lifecycle.getEffectiveDataRetention(globalRetention))
                .getConditions()
                .keySet()) {
                assertThat(serialized, containsString(label));
            }
            // We check that even if there was no retention provided by the user, the global retention applies
            assertThat(serialized, not(containsString("data_retention")));
            assertThat(serialized, containsString("effective_retention"));
        }
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(IndicesModule.getNamedWriteables());
    }

    private static Map<String, ComponentTemplate> randomTemplates() {
        Map<String, ComponentTemplate> templates = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            templates.put(randomAlphaOfLength(4), ComponentTemplateTests.randomInstance());
        }
        return templates;
    }
}
