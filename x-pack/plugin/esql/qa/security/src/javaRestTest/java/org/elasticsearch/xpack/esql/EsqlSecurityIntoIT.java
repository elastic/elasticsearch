/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.rest.ESRestTestCase.basicAuthHeaderValue;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Authorization of the destination of {@code INTO}. It has a cluster of its own because it needs batch indexing, which the other
 * security suites do not.
 */
public class EsqlSecurityIntoIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("indices.batch_indexing", "true")
        .rolesFile(Resource.fromClasspath("into-roles.yml"))
        .user("test-admin", "x-pack-test-password", "test-admin", true)
        .user("into_read_only_user", "x-pack-test-password", "into_read_only", false)
        .user("into_writer_user", "x-pack-test-password", "into_writer", false)
        .build();

    @Before
    public void requiresSnapshot() {
        assumeTrue(
            "INTO needs a snapshot build",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.INTO_COMMAND.capabilityName()))
        );
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

        ResponseException existing = expectThrows(ResponseException.class, () -> into("into_read_only_user", "into-existing"));
        ResponseException missing = expectThrows(ResponseException.class, () -> into("into_read_only_user", "into-missing"));
        String existingBody = EntityUtils.toString(existing.getResponse().getEntity());
        String missingBody = EntityUtils.toString(missing.getResponse().getEntity());
        assertThat(existingBody, existing.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(missingBody, missing.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(existingBody, containsString("is unauthorized for user [into_read_only_user]"));
        assertThat(existingBody, not(containsString("pipeline")));
        assertThat(existingBody.replace("into-existing", "<dest>"), equalTo(missingBody.replace("into-missing", "<dest>")));
    }

    /**
     * A writer passes the authorization without any read access to the destination, and may then be told what a bulk would tell
     * them: that the pipeline of the destination cannot be used. A missing destination that the writer may create is written.
     */
    public void testWriterWithoutReadAccessIsAuthorized() throws IOException {
        createDestinationWithPipeline();

        ResponseException refusal = expectThrows(ResponseException.class, () -> into("into_writer_user", "into-existing"));
        String body = EntityUtils.toString(refusal.getResponse().getEntity());
        assertThat(body, refusal.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("does not support ingest pipelines"));

        assertThat(into("into_writer_user", "into-missing").getStatusLine().getStatusCode(), equalTo(200));
    }

    private void createDestinationWithPipeline() throws IOException {
        Request pipeline = new Request("PUT", "_ingest/pipeline/into-pipeline");
        pipeline.setJsonEntity("{\"processors\": []}");
        assertOK(adminClient().performRequest(pipeline));
        createIndex(adminClient(), "into-existing", Settings.builder().put("index.default_pipeline", "into-pipeline").build());
    }

    private Response into(String user, String destination) throws IOException {
        Request request = new Request("POST", "_query");
        request.setJsonEntity("{\"query\": \"ROW x = 1 | INTO " + destination + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        request.addParameter("error_trace", "true");
        return client().performRequest(request);
    }
}
