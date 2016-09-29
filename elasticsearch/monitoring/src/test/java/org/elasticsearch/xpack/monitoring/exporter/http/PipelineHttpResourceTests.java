/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;

/**
 * Tests {@link PipelineHttpResource}.
 */
public class PipelineHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final String pipelineName = ".my_pipeline";
    private final byte[] pipelineBytes = new byte[] { randomByte(), randomByte(), randomByte() };
    private final Supplier<byte[]> pipeline = () -> pipelineBytes;

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

    public void testDoCheckTrue() throws IOException {
        assertCheckExists(resource, "/_ingest/pipeline", pipelineName);
    }

    public void testDoCheckFalse() throws IOException {
        assertCheckDoesNotExist(resource, "/_ingest/pipeline", pipelineName);
    }

    public void testDoCheckNullWithException() throws IOException {
        assertCheckWithException(resource, "/_ingest/pipeline", pipelineName);
    }

    public void testDoPublishTrue() throws IOException {
        assertPublishSucceeds(resource, "/_ingest/pipeline", pipelineName, ByteArrayEntity.class);
    }

    public void testDoPublishFalse() throws IOException {
        assertPublishFails(resource, "/_ingest/pipeline", pipelineName, ByteArrayEntity.class);
    }

    public void testDoPublishFalseWithException() throws IOException {
        assertPublishWithException(resource, "/_ingest/pipeline", pipelineName, ByteArrayEntity.class);
    }

    public void testParameters() {
        assertParameters(resource);
    }

}
