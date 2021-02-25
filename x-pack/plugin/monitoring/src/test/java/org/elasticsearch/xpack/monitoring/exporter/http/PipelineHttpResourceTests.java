/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.Version;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;

/**
 * Tests {@link PipelineHttpResource}.
 */
public class PipelineHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final String pipelineName = ".my_pipeline";
    private final byte[] pipelineBytes = new byte[] { randomByte(), randomByte(), randomByte() };
    private final Supplier<byte[]> pipeline = () -> pipelineBytes;
    private final int minimumVersion = Math.min(MonitoringTemplateUtils.LAST_UPDATED_VERSION, Version.CURRENT.id);

    private final PipelineHttpResource resource = new PipelineHttpResource(owner, masterTimeout, pipelineName, pipeline);

    public void testPipelineToHttpEntity() throws IOException {
        final HttpEntity entity = resource.pipelineToHttpEntity();

        assertThat(entity.getContentType().getValue(), is(ContentType.APPLICATION_JSON.toString()));

        final InputStream byteStream = entity.getContent();

        assertThat(byteStream.available(), is(pipelineBytes.length));

        for (final byte pipelineByte : pipelineBytes) {
            assertThat(pipelineByte, is((byte)byteStream.read()));
        }

        assertThat(byteStream.available(), is(0));
    }

    public void testDoCheckExists() {
        final HttpEntity entity = entityForResource(true, pipelineName, minimumVersion);

        doCheckWithStatusCode(resource, "/_ingest/pipeline", pipelineName, successfulCheckStatus(), true, entity);
    }

    public void testDoCheckDoesNotExist() {
        if (randomBoolean()) {
            // it does not exist because it's literally not there
            assertCheckDoesNotExist(resource, "/_ingest/pipeline", pipelineName);
        } else {
            // it does not exist because we need to replace it
            final HttpEntity entity = entityForResource(false, pipelineName, minimumVersion);

            doCheckWithStatusCode(resource, "/_ingest/pipeline", pipelineName,
                                  successfulCheckStatus(), false, entity);
        }
    }

    public void testDoCheckError() {
        if (randomBoolean()) {
            // error because of a server error
            assertCheckWithException(resource, "/_ingest/pipeline", pipelineName);
        } else {
            // error because of a malformed response
            final HttpEntity entity = entityForResource(null, pipelineName, minimumVersion);

            doCheckWithStatusCode(resource, "/_ingest/pipeline", pipelineName, successfulCheckStatus(), null, entity);
        }
    }

    public void testDoPublishTrue() {
        assertPublishSucceeds(resource, "/_ingest/pipeline", pipelineName, Collections.emptyMap(), ByteArrayEntity.class);
    }

    public void testDoPublishFalseWithException() {
        assertPublishWithException(resource, "/_ingest/pipeline", pipelineName, Collections.emptyMap(), ByteArrayEntity.class);
    }

    public void testParameters() {
        assertVersionParameters(resource);
    }

}
