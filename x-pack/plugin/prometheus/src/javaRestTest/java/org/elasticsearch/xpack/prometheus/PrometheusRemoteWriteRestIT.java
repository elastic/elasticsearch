/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.prometheus.proto.RemoteWriteProtos;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PrometheusRemoteWriteRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .systemProperty("es.prometheus_feature_flag_enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testRemoteWriteEndpointWithEmptyRequest() throws Exception {
        RemoteWriteProtos.WriteRequest writeRequest = RemoteWriteProtos.WriteRequest.newBuilder().build();

        Response response = sendRemoteWriteRequest(writeRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(204));
    }

    public void testRemoteWriteEndpointWithTimeseries() throws Exception {
        RemoteWriteProtos.WriteRequest writeRequest = RemoteWriteProtos.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWriteProtos.TimeSeries.newBuilder()
                    .addLabels(RemoteWriteProtos.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                    .addLabels(RemoteWriteProtos.Label.newBuilder().setName("job").setValue("test_job").build())
                    .addSamples(RemoteWriteProtos.Sample.newBuilder().setValue(42.0).setTimestamp(System.currentTimeMillis()).build())
                    .build()
            )
            .build();

        Response response = sendRemoteWriteRequest(writeRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(204));
    }

    public void testRemoteWriteEndpointWithMultipleTimeseries() throws Exception {
        long now = System.currentTimeMillis();
        RemoteWriteProtos.WriteRequest writeRequest = RemoteWriteProtos.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWriteProtos.TimeSeries.newBuilder()
                    .addLabels(RemoteWriteProtos.Label.newBuilder().setName("__name__").setValue("metric_one").build())
                    .addSamples(RemoteWriteProtos.Sample.newBuilder().setValue(1.0).setTimestamp(now).build())
                    .build()
            )
            .addTimeseries(
                RemoteWriteProtos.TimeSeries.newBuilder()
                    .addLabels(RemoteWriteProtos.Label.newBuilder().setName("__name__").setValue("metric_two").build())
                    .addSamples(RemoteWriteProtos.Sample.newBuilder().setValue(2.0).setTimestamp(now).build())
                    .build()
            )
            .build();

        Response response = sendRemoteWriteRequest(writeRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(204));
    }

    private Response sendRemoteWriteRequest(RemoteWriteProtos.WriteRequest writeRequest) throws IOException {
        Request request = new Request("POST", "/_prometheus/api/v1/write");
        byte[] body = writeRequest.toByteArray();
        request.setEntity(new ByteArrayEntity(body, ContentType.create("application/x-protobuf")));
        return client().performRequest(request);
    }
}
