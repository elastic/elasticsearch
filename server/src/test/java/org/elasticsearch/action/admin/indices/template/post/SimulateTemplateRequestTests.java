/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SimulateTemplateRequestTests extends ESTestCase {

    public void testAddingGlobalTemplateWithHiddenIndexSettingIsIllegal() {
        Template template = new Template(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(), null, null);
        ComposableIndexTemplate globalTemplate = ComposableIndexTemplate.builder().indexPatterns(List.of("*")).template(template).build();

        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("test");
        request.indexTemplate(globalTemplate);

        SimulateTemplateAction.Request simulateRequest = new SimulateTemplateAction.Request(TEST_REQUEST_TIMEOUT, "testing");
        simulateRequest.indexTemplateRequest(request);

        ActionRequestValidationException validationException = simulateRequest.validate();
        assertThat(validationException, is(notNullValue()));
        List<String> validationErrors = validationException.validationErrors();
        assertThat(validationErrors.size(), is(1));
        String error = validationErrors.get(0);
        assertThat(error, is("global composable templates may not specify the setting " + IndexMetadata.SETTING_INDEX_HIDDEN));
    }
}
