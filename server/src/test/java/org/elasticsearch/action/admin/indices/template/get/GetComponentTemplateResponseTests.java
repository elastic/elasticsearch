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
