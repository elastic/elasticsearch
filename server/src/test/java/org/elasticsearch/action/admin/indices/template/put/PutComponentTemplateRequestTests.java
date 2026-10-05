/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.ResettableValue;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class PutComponentTemplateRequestTests extends ESTestCase {

    public void testValidateRejectsFrozenAfterOnFailureStoreLifecycle() {
        DataStreamLifecycle.Template failureLifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .frozenAfter(new TimeValue(30, TimeUnit.DAYS))
            .buildTemplate();
        DataStreamFailureStore.Template failureStore = DataStreamFailureStore.builder()
            .lifecycle(ResettableValue.create(failureLifecycle))
            .buildTemplate();
        DataStreamOptions.Template dataStreamOptions = new DataStreamOptions.Template(ResettableValue.create(failureStore));
        Template template = new Template(null, null, null, null, dataStreamOptions);
        ComponentTemplate componentTemplate = new ComponentTemplate(template, null, null);

        PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request("test");
        request.componentTemplate(componentTemplate);

        ActionRequestValidationException validationException = request.validate();

        assertNotNull(validationException);
        assertThat(
            validationException.getMessage(),
            containsString(DataStreamLifecycle.FROZEN_AFTER_NOT_SUPPORTED_ON_FAILURES_ERROR_MESSAGE)
        );
    }

    public void testValidateAcceptsExplicitFrozenAfterResetOnFailureStoreLifecycle() {
        DataStreamLifecycle.Template failureLifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .frozenAfter(ResettableValue.reset())
            .buildTemplate();
        DataStreamFailureStore.Template failureStore = DataStreamFailureStore.builder()
            .lifecycle(ResettableValue.create(failureLifecycle))
            .buildTemplate();
        DataStreamOptions.Template dataStreamOptions = new DataStreamOptions.Template(ResettableValue.create(failureStore));
        Template template = new Template(null, null, null, null, dataStreamOptions);
        ComponentTemplate componentTemplate = new ComponentTemplate(template, null, null);

        PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request("test");
        request.componentTemplate(componentTemplate);

        assertThat(request.validate(), nullValue());
    }
}
