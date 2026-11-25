/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Authorization of the destination of {@code INTO}. It has a cluster of its own because it needs batch indexing, which the other
 * security suites do not.
 */
public class EsqlSecurityIntoIT extends ESRestTestCase {
    public static final String USER = "into_read_only_user";
    public static final String EXISTING = "into-existing";
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .feature(FeatureFlag.BATCH_INDEXING)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("indices.batch_indexing", "true")
        .rolesFile(Resource.fromClasspath("into-roles.yml"))
        .user("test-admin", "x-pack-test-password", "test-admin", true)
        .user(USER, "x-pack-test-password", "into_read_only", false)
        .user("into_writer_user", "x-pack-test-password", "into_writer", false)
        .build();

    @Before
    public void requiresIntoCommand() {
        assumeTrue("INTO command required", hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.INTO_COMMAND.capabilityName())));
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * A ROW source needs no index privilege, so INTO authorizes the destination itself: a read-only caller gets a 403 that is the
     * same for an existing and a missing destination.
     */
    public void testDestinationDeniedWithoutWriteAccess() throws IOException {
        createDestinationWithPipeline();

        ResponseException existing = expectThrows(ResponseException.class, () -> into(USER, EXISTING));
        ResponseException missing = expectThrows(ResponseException.class, () -> into(USER, "into-missing"));
        String existingBody = EntityUtils.toString(existing.getResponse().getEntity());
        String missingBody = EntityUtils.toString(missing.getResponse().getEntity());
        assertThat(existingBody, existing.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
        assertThat(missingBody, missing.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
        String format = "action [indices:data/write/index:op_type/create] is unauthorized for user [%s] on indices [%s]";
        assertThat(existingBody, containsString(Strings.format(format, USER, EXISTING)));
        assertThat(missingBody, containsString(Strings.format(format, USER, "into-missing")));
    }

    /**
     * A writer passes the authorization without any read access to the destination, and may then be told what a bulk would tell
     * them: that the pipeline of the destination cannot be used. A missing destination is an error for them too, as INTO does not
     * create indices.
     */
    public void testWriterWithoutReadAccessIsAuthorized() throws IOException {
        createDestinationWithPipeline();

        ResponseException refusal = expectThrows(ResponseException.class, () -> into("into_writer_user", EXISTING));
        String body = EntityUtils.toString(refusal.getResponse().getEntity());
        assertThat(body, refusal.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
        assertThat(body, containsString("does not support ingest pipelines"));

        ResponseException missing = expectThrows(ResponseException.class, () -> into("into_writer_user", "into-missing"));
        assertThat(missing.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
        assertThat(EntityUtils.toString(missing.getResponse().getEntity()), containsString("INTO destination [into-missing] does not exist"));
    }

    private void createDestinationWithPipeline() throws IOException {
        Request pipeline = new Request("PUT", "_ingest/pipeline/into-pipeline");
        pipeline.setJsonEntity("{\"processors\": []}");
        assertOK(adminClient().performRequest(pipeline));
        createIndex(adminClient(), EXISTING, Settings.builder().put("index.default_pipeline", "into-pipeline").build());
    }

    private Response into(String user, String destination) throws IOException {
        Request request = new Request("POST", "_query");
        request.setJsonEntity("{\"query\": \"ROW x = 1 | INTO " + destination + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        request.addParameter("error_trace", "true");
        return client().performRequest(request);
    }
}
