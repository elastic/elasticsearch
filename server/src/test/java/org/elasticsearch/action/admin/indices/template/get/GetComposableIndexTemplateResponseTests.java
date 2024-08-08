/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfigurationTests;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateTests;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.Map;

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

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(IndicesModule.getNamedWriteables());
    }
}
