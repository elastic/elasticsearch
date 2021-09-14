/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComponentTemplateTests;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetComponentTemplateResponseTests extends AbstractWireSerializingTestCase<GetComponentTemplateAction.Response> {
    @Override
    protected Writeable.Reader<GetComponentTemplateAction.Response> instanceReader() {
        return GetComponentTemplateAction.Response::new;
    }

    @Override
    protected GetComponentTemplateAction.Response createTestInstance() {
        if (randomBoolean()) {
            return new GetComponentTemplateAction.Response(Collections.emptyMap());
        }
        Map<String, ComponentTemplate> templates = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            templates.put(randomAlphaOfLength(4), ComponentTemplateTests.randomInstance());
        }
        return new GetComponentTemplateAction.Response(templates);
    }

    @Override
    protected GetComponentTemplateAction.Response mutateInstance(GetComponentTemplateAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
