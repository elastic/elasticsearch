/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateTests;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class PutComposableIndexTemplateRequestTests extends AbstractWireSerializingTestCase<PutComposableIndexTemplateAction.Request> {
    @Override
    protected Writeable.Reader<PutComposableIndexTemplateAction.Request> instanceReader() {
        return PutComposableIndexTemplateAction.Request::new;
    }

    @Override
    protected PutComposableIndexTemplateAction.Request createTestInstance() {
        PutComposableIndexTemplateAction.Request req = new PutComposableIndexTemplateAction.Request(randomAlphaOfLength(4));
        req.cause(randomAlphaOfLength(4));
        req.create(randomBoolean());
        req.indexTemplate(ComposableIndexTemplateTests.randomInstance());
        return req;
    }

    @Override
    protected PutComposableIndexTemplateAction.Request mutateInstance(PutComposableIndexTemplateAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public void testPutGlobalTemplatesCannotHaveHiddenIndexSetting() {
        Template template = new Template(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(), null, null);
        ComposableIndexTemplate globalTemplate = new ComposableIndexTemplate.Builder().indexPatterns(List.of("*"))
            .template(template).build();

        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("test");
        request.indexTemplate(globalTemplate);

        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, is(notNullValue()));
        List<String> validationErrors = validationException.validationErrors();
        assertThat(validationErrors.size(), is(1));
        String error = validationErrors.get(0);
        assertThat(error, is("global composable templates may not specify the setting " + IndexMetadata.SETTING_INDEX_HIDDEN));
    }

    public void testPutIndexTemplateV2RequestMustContainTemplate() {
        PutComposableIndexTemplateAction.Request requestWithoutTemplate = new PutComposableIndexTemplateAction.Request("test");

        ActionRequestValidationException validationException = requestWithoutTemplate.validate();
        assertThat(validationException, is(notNullValue()));
        List<String> validationErrors = validationException.validationErrors();
        assertThat(validationErrors.size(), is(1));
        String error = validationErrors.get(0);
        assertThat(error, is("an index template is required"));
    }

    public void testValidationOfPriority() {
        PutComposableIndexTemplateAction.Request req = new PutComposableIndexTemplateAction.Request("test");
        req.indexTemplate(new ComposableIndexTemplate.Builder().indexPatterns(Arrays.asList("foo", "bar"))
           .priority(-5L).build());
        ActionRequestValidationException validationException = req.validate();
        assertThat(validationException, is(notNullValue()));
        List<String> validationErrors = validationException.validationErrors();
        assertThat(validationErrors.size(), is(1));
        String error = validationErrors.get(0);
        assertThat(error, is("index template priority must be >= 0"));
    }

    public void testValidateNoTemplate() {
        PutComposableIndexTemplateAction.Request req = new PutComposableIndexTemplateAction.Request("test");
        req.indexTemplate(new ComposableIndexTemplate.Builder()
            .indexPatterns(Collections.singletonList("*"))
            .build());
        assertNull(req.validate());

        req.indexTemplate(new ComposableIndexTemplate.Builder()
            .indexPatterns(Collections.singletonList("*"))
            .template(new Template(null, null, null))
            .build());
        assertNull(req.validate());
    }
}
