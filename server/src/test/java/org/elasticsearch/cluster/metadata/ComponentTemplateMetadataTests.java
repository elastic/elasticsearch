/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ComponentTemplateMetadataTests extends AbstractNamedWriteableTestCase<ComponentTemplateMetadata> {

    @Override
    protected ComponentTemplateMetadata createTestInstance() {
        int count = randomIntBetween(0, 3);
        Map<String, ComponentTemplate> templateMap = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            templateMap.put(randomAlphaOfLength(4), ComponentTemplateTests.randomInstance());
        }
        return new ComponentTemplateMetadata(templateMap);
    }

    @Override
    protected ComponentTemplateMetadata mutateInstance(ComponentTemplateMetadata instance) throws IOException {
        if (instance.componentTemplates().size() == 0) {
            // Not really much to mutate, so just generate a new one
            return randomValueOtherThan(instance, this::createTestInstance);
        }
        Map<String, ComponentTemplate> templates = new HashMap<>(instance.componentTemplates());
        Map.Entry<String, ComponentTemplate> newTemplate = instance.componentTemplates().entrySet().iterator().next();
        if (randomBoolean()) {
            // Change the key
            templates.put(randomAlphaOfLength(4), newTemplate.getValue());
        } else {
            // Change the value
            templates.put(newTemplate.getKey(), ComponentTemplateTests.mutateTemplate(newTemplate.getValue()));
        }
        return new ComponentTemplateMetadata(templates);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(new NamedWriteableRegistry.Entry(ComponentTemplateMetadata.class,
            ComponentTemplateMetadata.TYPE, ComponentTemplateMetadata::new)));
    }

    @Override
    protected Class<ComponentTemplateMetadata> categoryClass() {
        return ComponentTemplateMetadata.class;
    }
}
