/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkResponse;
import org.elasticsearch.xpack.monitoring.exporter.ExportException;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MonitoringBulkResponseTests extends ESTestCase {

    public void testResponseStatus() {
        final long took = Math.abs(randomLong());

        MonitoringBulkResponse response = new MonitoringBulkResponse(took, false);

        assertThat(response.getTookInMillis(), equalTo(took));
        assertThat(response.getError(), is(nullValue()));
        assertThat(response.isIgnored(), is(false));
        assertThat(response.status(), equalTo(RestStatus.OK));

        response = new MonitoringBulkResponse(took, true);

        assertThat(response.getTookInMillis(), equalTo(took));
        assertThat(response.getError(), is(nullValue()));
        assertThat(response.isIgnored(), is(true));
        assertThat(response.status(), equalTo(RestStatus.OK));

        ExportException exception = new ExportException(randomAlphaOfLength(10));
        response = new MonitoringBulkResponse(took, new MonitoringBulkResponse.Error(exception));

        assertThat(response.getTookInMillis(), equalTo(took));
        assertThat(response.getError(), is(notNullValue()));
        assertThat(response.isIgnored(), is(false));
        assertThat(response.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 50);
        for (int i = 0; i < iterations; i++) {
            MonitoringBulkResponse response;
            if (randomBoolean()) {
                response = new MonitoringBulkResponse(Math.abs(randomLong()), randomBoolean());
            } else {
                Exception exception = randomFrom(
                    new ExportException(randomAlphaOfLength(5), new IllegalStateException(randomAlphaOfLength(5))),
                    new IllegalStateException(randomAlphaOfLength(5)),
                    new IllegalArgumentException(randomAlphaOfLength(5))
                );
                response = new MonitoringBulkResponse(Math.abs(randomLong()), new MonitoringBulkResponse.Error(exception));
            }

            final Version version = VersionUtils.randomVersion(random());
            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(version);
            response.writeTo(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(version);
            MonitoringBulkResponse response2 = new MonitoringBulkResponse(streamInput);
            assertThat(response2.getTookInMillis(), equalTo(response.getTookInMillis()));
            if (response.getError() == null) {
                assertThat(response2.getError(), is(nullValue()));
            } else {
                assertThat(response2.getError(), is(notNullValue()));
            }
            assertThat(response2.isIgnored(), is(response.isIgnored()));
        }
    }
}
