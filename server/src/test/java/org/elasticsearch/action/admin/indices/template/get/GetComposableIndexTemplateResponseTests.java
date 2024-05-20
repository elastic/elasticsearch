/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateTests;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionTests;
import org.elasticsearch.common.io.stream.Writeable;
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
        DataStreamGlobalRetention globalRetention = randomBoolean() ? null : DataStreamGlobalRetentionTests.randomGlobalRetention();
        if (randomBoolean()) {
            return new GetComposableIndexTemplateAction.Response(Map.of(), globalRetention);
        }
        Map<String, ComposableIndexTemplate> templates = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            templates.put(randomAlphaOfLength(4), ComposableIndexTemplateTests.randomInstance());
        }
        return new GetComposableIndexTemplateAction.Response(templates, globalRetention);
    }

    @Override
    protected GetComposableIndexTemplateAction.Response mutateInstance(GetComposableIndexTemplateAction.Response instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
