/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateV2Action;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.IndexTemplateV2Tests;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SimulateIndexTemplateRequestTests extends AbstractWireSerializingTestCase<SimulateIndexTemplateRequest> {

    @Override
    protected Writeable.Reader<SimulateIndexTemplateRequest> instanceReader() {
        return SimulateIndexTemplateRequest::new;
    }

    @Override
    protected SimulateIndexTemplateRequest createTestInstance() {
        SimulateIndexTemplateRequest req = new SimulateIndexTemplateRequest(randomAlphaOfLength(10));
        PutIndexTemplateV2Action.Request newTemplateRequest = new PutIndexTemplateV2Action.Request(randomAlphaOfLength(4));
        newTemplateRequest.indexTemplate(IndexTemplateV2Tests.randomInstance());
        req.indexTemplateRequest(newTemplateRequest);
        return req;
    }

    @Override
    protected SimulateIndexTemplateRequest mutateInstance(SimulateIndexTemplateRequest instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public void testIndexNameCannotBeNullOrEmpty() {
        expectThrows(IllegalArgumentException.class, () -> new SimulateIndexTemplateRequest((String) null));
        expectThrows(IllegalArgumentException.class, () -> new SimulateIndexTemplateRequest(""));
    }

    public void testAddingGlobalTemplateWithHiddenIndexSettingIsIllegal() {
        Template template = new Template(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(), null, null);
        IndexTemplateV2 globalTemplate = new IndexTemplateV2(List.of("*"), template, null, null, null, null, null);

        PutIndexTemplateV2Action.Request request = new PutIndexTemplateV2Action.Request("test");
        request.indexTemplate(globalTemplate);

        SimulateIndexTemplateRequest simulateRequest = new SimulateIndexTemplateRequest("testing");
        simulateRequest.indexTemplateRequest(request);

        ActionRequestValidationException validationException = simulateRequest.validate();
        assertThat(validationException, is(notNullValue()));
        List<String> validationErrors = validationException.validationErrors();
        assertThat(validationErrors.size(), is(1));
        String error = validationErrors.get(0);
        assertThat(error, is("global V2 templates may not specify the setting " + IndexMetadata.SETTING_INDEX_HIDDEN));
    }
}
