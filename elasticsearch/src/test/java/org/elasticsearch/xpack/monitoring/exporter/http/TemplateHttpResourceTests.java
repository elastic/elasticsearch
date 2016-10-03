/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;

/**
 * Tests {@link TemplateHttpResource}.
 */
public class TemplateHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final String templateName = ".my_template";
    private final String templateValue = "{\"template\":\".xyz-*\",\"mappings\":{}}";
    private final Supplier<String> template = () -> templateValue;

    private final TemplateHttpResource resource = new TemplateHttpResource(owner, masterTimeout, templateName, template);

    public void testPipelineToHttpEntity() throws IOException {
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

    public void testDoCheckTrue() throws IOException {
        assertCheckExists(resource, "/_template", templateName);
    }

    public void testDoCheckFalse() throws IOException {
        assertCheckDoesNotExist(resource, "/_template", templateName);
    }

    public void testDoCheckNullWithException() throws IOException {
        assertCheckWithException(resource, "/_template", templateName);
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
        assertParameters(resource);
    }

}
