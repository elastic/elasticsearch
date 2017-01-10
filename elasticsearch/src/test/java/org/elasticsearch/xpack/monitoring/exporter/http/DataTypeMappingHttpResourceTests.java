/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils.DATA_INDEX;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link DataTypeMappingHttpResource}.
 */
public class DataTypeMappingHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final String typeName = "my_type";

    private final DataTypeMappingHttpResource resource = new DataTypeMappingHttpResource(owner, masterTimeout, typeName);

    public void testDoCheckTrueFor404() throws IOException {
        // if the index is not there, then we don't need to manually add the type
        doCheckWithStatusCode(resource, "/" + DATA_INDEX +  "/_mapping", typeName, notFoundCheckStatus(), CheckResponse.EXISTS);
    }

    public void testDoCheckTrue() throws IOException {
        final String endpoint = "/" + DATA_INDEX +  "/_mapping/" + typeName;
        // success does't mean it exists unless the mapping exists! it returns {} if the index exists, but the type does not
        final Response response = response("GET", endpoint, successfulCheckStatus());
        final HttpEntity responseEntity = mock(HttpEntity.class);
        final long validMapping = randomIntBetween(3, Integer.MAX_VALUE);

        when(response.getEntity()).thenReturn(responseEntity);
        when(responseEntity.getContentLength()).thenReturn(validMapping);

        doCheckWithStatusCode(resource, endpoint, CheckResponse.EXISTS, response);

        verify(responseEntity).getContentLength();
    }

    public void testDoCheckFalse() throws IOException {
        final String endpoint = "/" + DATA_INDEX +  "/_mapping/" + typeName;
        // success does't mean it exists unless the mapping exists! it returns {} if the index exists, but the type does not
        final Response response = response("GET", endpoint, successfulCheckStatus());
        final HttpEntity responseEntity = mock(HttpEntity.class);
        final long invalidMapping = randomIntBetween(Integer.MIN_VALUE, 2);

        when(response.getEntity()).thenReturn(responseEntity);
        when(responseEntity.getContentLength()).thenReturn(invalidMapping);

        doCheckWithStatusCode(resource, endpoint, CheckResponse.DOES_NOT_EXIST, response);

        verify(responseEntity).getContentLength();
    }

    public void testDoCheckNullWithException() throws IOException {
        assertCheckWithException(resource, "/" + DATA_INDEX +  "/_mapping", typeName);
    }

    public void testDoPublishTrue() throws IOException {
        assertPublishSucceeds(resource, "/" + DATA_INDEX +  "/_mapping", typeName, StringEntity.class);
    }

    public void testDoPublishFalse() throws IOException {
        assertPublishFails(resource, "/" + DATA_INDEX +  "/_mapping", typeName, StringEntity.class);
    }

    public void testDoPublishFalseWithException() throws IOException {
        assertPublishWithException(resource, "/" + DATA_INDEX +  "/_mapping", typeName, StringEntity.class);
    }

    public void testParameters() {
        final Map<String, String> parameters = resource.getParameters();

        if (masterTimeout != null) {
            assertThat(parameters.get("master_timeout"), is(masterTimeout.toString()));
        }

        assertThat(parameters.size(), is(masterTimeout == null ? 0 : 1));
    }

}
