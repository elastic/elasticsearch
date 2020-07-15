/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateTests;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SimulateTemplateRequestTests extends AbstractWireSerializingTestCase<SimulateTemplateAction.Request> {

    @Override
    protected Writeable.Reader<SimulateTemplateAction.Request> instanceReader() {
        return SimulateTemplateAction.Request::new;
    }

    @Override
    protected SimulateTemplateAction.Request createTestInstance() {
        SimulateTemplateAction.Request req = new SimulateTemplateAction.Request(randomAlphaOfLength(10));
        PutComposableIndexTemplateAction.Request newTemplateRequest = new PutComposableIndexTemplateAction.Request(randomAlphaOfLength(4));
        newTemplateRequest.indexTemplate(ComposableIndexTemplateTests.randomInstance());
        req.indexTemplateRequest(newTemplateRequest);
        return req;
    }

    @Override
    protected SimulateTemplateAction.Request mutateInstance(SimulateTemplateAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public void testIndexNameCannotBeNullOrEmpty() {
        expectThrows(IllegalArgumentException.class, () -> new SimulateTemplateAction.Request((String) null));
        expectThrows(IllegalArgumentException.class,
            () -> new SimulateTemplateAction.Request((PutComposableIndexTemplateAction.Request) null));
    }

    public void testAddingGlobalTemplateWithHiddenIndexSettingIsIllegal() {
        Template template = new Template(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(), null, null);
        ComposableIndexTemplate globalTemplate = new ComposableIndexTemplate(List.of("*"), template, null, null, null, null, null);

        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("test");
        request.indexTemplate(globalTemplate);

        SimulateTemplateAction.Request simulateRequest = new SimulateTemplateAction.Request("testing");
        simulateRequest.indexTemplateRequest(request);

        ActionRequestValidationException validationException = simulateRequest.validate();
        assertThat(validationException, is(notNullValue()));
        List<String> validationErrors = validationException.validationErrors();
        assertThat(validationErrors.size(), is(1));
        String error = validationErrors.get(0);
        assertThat(error, is("global composable templates may not specify the setting " + IndexMetadata.SETTING_INDEX_HIDDEN));
    }
}
