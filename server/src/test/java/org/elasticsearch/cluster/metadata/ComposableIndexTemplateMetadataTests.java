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

public class ComposableIndexTemplateMetadataTests extends AbstractNamedWriteableTestCase<ComposableIndexTemplateMetadata> {
    @Override
    protected ComposableIndexTemplateMetadata createTestInstance() {
        if (randomBoolean()) {
            return new ComposableIndexTemplateMetadata(Collections.emptyMap());
        }
        Map<String, ComposableIndexTemplate> templates = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            templates.put(randomAlphaOfLength(5), ComposableIndexTemplateTests.randomInstance());
        }
        return new ComposableIndexTemplateMetadata(templates);
    }

    @Override
    protected ComposableIndexTemplateMetadata mutateInstance(ComposableIndexTemplateMetadata instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(new NamedWriteableRegistry.Entry(ComposableIndexTemplateMetadata.class,
            ComposableIndexTemplateMetadata.TYPE, ComposableIndexTemplateMetadata::new)));
    }

    @Override
    protected Class<ComposableIndexTemplateMetadata> categoryClass() {
        return ComposableIndexTemplateMetadata.class;
    }
}
