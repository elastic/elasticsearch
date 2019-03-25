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
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;

/**
 * Tests {@link TemplateHttpResource}.
 */
public class TemplateHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final String templateName = ".my_template";

    //the internal representation has the type, the external representation should not
    private final String templateValueInternal = "{\"order\":0,\"index_patterns\":[\".xyz-*\"],\"settings\":{},\"mappings\":{\"_doc\"" +
        ":{\"properties\":{\"one\":{\"properties\":{\"two\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}}}},\"aliases\":{}}";
    private final String templateValueExternal = "{\"order\":0,\"index_patterns\":[\".xyz-*\"],\"settings\":{},\"mappings\"" +
        ":{\"properties\":{\"one\":{\"properties\":{\"two\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}}},\"aliases\":{}}";
    private final Supplier<String> template = () -> templateValueInternal;
    private final int minimumVersion = Math.min(MonitoringTemplateUtils.LAST_UPDATED_VERSION, Version.CURRENT.id);

    private final TemplateHttpResource resource = new TemplateHttpResource(owner, masterTimeout, templateName, template);

    public void testTemplateToHttpEntity() throws IOException {
        //the internal representation is converted to the external representation for the resource
        final byte[] templateValueBytes = templateValueExternal.getBytes(ContentType.APPLICATION_JSON.getCharset());
        final HttpEntity entity = resource.templateToHttpEntity();

        assertThat(entity.getContentType().getValue(), is(ContentType.APPLICATION_JSON.toString()));

        final InputStream byteStream = entity.getContent();

        assertThat(byteStream.available(), is(templateValueBytes.length));

        for (final byte templateByte : templateValueBytes) {
            assertThat(templateByte, is((byte)byteStream.read()));
        }

        assertThat(byteStream.available(), is(0));
    }

    public void testDoCheckExists() {
        final HttpEntity entity = entityForResource(true, templateName, minimumVersion);

        doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), true, entity);
    }

    public void testDoCheckDoesNotExist() {
        if (randomBoolean()) {
            // it does not exist because it's literally not there
            assertCheckDoesNotExist(resource, "/_template", templateName);
        } else {
            // it does not exist because we need to replace it
            final HttpEntity entity = entityForResource(false, templateName, minimumVersion);

            doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), false, entity);
        }
    }

    public void testDoCheckError() {
        if (randomBoolean()) {
            // error because of a server error
            assertCheckWithException(resource, "/_template", templateName);
        } else {
            // error because of a malformed response
            final HttpEntity entity = entityForResource(null, templateName, minimumVersion);

            doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), null, entity);
        }
    }

    public void testDoPublishTrue() {
        assertPublishSucceeds(resource, "/_template", templateName, Collections.emptyMap(), StringEntity.class);
    }

    public void testDoPublishFalseWithException() {
        assertPublishWithException(resource, "/_template", templateName, Collections.emptyMap(), StringEntity.class);
    }

    public void testParameters() {
        assertVersionParameters(resource);
    }

}
