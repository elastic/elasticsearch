/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateTests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

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
        TransportPutComposableIndexTemplateAction.Request newTemplateRequest = new TransportPutComposableIndexTemplateAction.Request(
            randomAlphaOfLength(4)
        );
        newTemplateRequest.indexTemplate(ComposableIndexTemplateTests.randomInstance());
        req.indexTemplateRequest(newTemplateRequest);
        req.includeDefaults(randomBoolean());
        return req;
    }

    @Override
    protected SimulateIndexTemplateRequest mutateInstance(SimulateIndexTemplateRequest instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public void testIndexNameCannotBeNullOrEmpty() {
        expectThrows(IllegalArgumentException.class, () -> new SimulateIndexTemplateRequest((String) null));
        expectThrows(IllegalArgumentException.class, () -> new SimulateIndexTemplateRequest(""));
    }

    public void testAddingGlobalTemplateWithHiddenIndexSettingIsIllegal() {
        Template template = new Template(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(), null, null);
        ComposableIndexTemplate globalTemplate = ComposableIndexTemplate.builder().indexPatterns(List.of("*")).template(template).build();

        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("test");
        request.indexTemplate(globalTemplate);

        SimulateIndexTemplateRequest simulateRequest = new SimulateIndexTemplateRequest("testing");
        simulateRequest.indexTemplateRequest(request);

        ActionRequestValidationException validationException = simulateRequest.validate();
        assertThat(validationException, is(notNullValue()));
        List<String> validationErrors = validationException.validationErrors();
        assertThat(validationErrors.size(), is(1));
        String error = validationErrors.get(0);
        assertThat(error, is("global composable templates may not specify the setting " + IndexMetadata.SETTING_INDEX_HIDDEN));
    }
}
