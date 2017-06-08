/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse.DOES_NOT_EXIST;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse.ERROR;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse.EXISTS;
import static org.hamcrest.Matchers.is;

/**
 * Tests {@link TemplateHttpResource}.
 */
public class TemplateHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final String templateName = ".my_template";
    private final String templateValue = "{\"template\":\".xyz-*\",\"mappings\":{}}";
    private final Supplier<String> template = () -> templateValue;
    private final int minimumVersion = Math.min(MonitoringTemplateUtils.LAST_UPDATED_VERSION, Version.CURRENT.id);

    private final TemplateHttpResource resource = new TemplateHttpResource(owner, masterTimeout, templateName, template);

    public void testTemplateToHttpEntity() throws IOException {
        final byte[] templateValueBytes = templateValue.getBytes(ContentType.APPLICATION_JSON.getCharset());
        final HttpEntity entity = resource.templateToHttpEntity();

        assertThat(entity.getContentType().getValue(), is(ContentType.APPLICATION_JSON.toString()));

        final InputStream byteStream = entity.getContent();

        assertThat(byteStream.available(), is(templateValueBytes.length));

        for (final byte templateByte : templateValueBytes) {
            assertThat(templateByte, is((byte)byteStream.read()));
        }

        assertThat(byteStream.available(), is(0));
    }

    public void testDoCheckExists() throws IOException {
        final HttpEntity entity = entityForResource(EXISTS, templateName, minimumVersion);

        doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), EXISTS, entity);
    }

    public void testDoCheckDoesNotExist() throws IOException {
        if (randomBoolean()) {
            // it does not exist because it's literally not there
            assertCheckDoesNotExist(resource, "/_template", templateName);
        } else {
            // it does not exist because we need to replace it
            final HttpEntity entity = entityForResource(DOES_NOT_EXIST, templateName, minimumVersion);

            doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), DOES_NOT_EXIST, entity);
        }
    }

    public void testDoCheckError() throws IOException {
        if (randomBoolean()) {
            // error because of a server error
            assertCheckWithException(resource, "/_template", templateName);
        } else {
            // error because of a malformed response
            final HttpEntity entity = entityForResource(ERROR, templateName, minimumVersion);

            doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), ERROR, entity);
        }
    }

    public void testDoPublishTrue() throws IOException {
        assertPublishSucceeds(resource, "/_template", templateName, StringEntity.class);
    }

    public void testDoPublishFalse() throws IOException {
        assertPublishFails(resource, "/_template", templateName, StringEntity.class);
    }

    public void testDoPublishFalseWithException() throws IOException {
        assertPublishWithException(resource, "/_template", templateName, StringEntity.class);
    }

    public void testParameters() {
        assertVersionParameters(resource);
    }

}
