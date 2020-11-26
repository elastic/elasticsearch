/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
