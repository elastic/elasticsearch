/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestResponseParsers;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ReindexWithSecurityIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = createCluster();

    public static ElasticsearchCluster createCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .configFile("http.key", Resource.fromClasspath("ssl/http.key"))
            .configFile("http.crt", Resource.fromClasspath("ssl/http.crt"))
            .configFile("ca.crt", Resource.fromClasspath("ssl/ca.crt"))
            .setting("reindex.remote.whitelist", "127.0.0.1:*")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.http.ssl.enabled", "true")
            .setting("xpack.security.http.ssl.certificate", "http.crt")
            .setting("xpack.security.http.ssl.key", "http.key")
            .setting("xpack.security.http.ssl.key_passphrase", "http-password")
            .setting("reindex.ssl.certificate_authorities", "ca.crt")
            .setting("xpack.security.autoconfiguration.enabled", "false")
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(USER, PASS, "superuser", false)
            .user("powerful_user", "x-pack-test-password", "superuser", false)
            .user("minimal_user", "x-pack-test-password", "minimal", false)
            .user("minimal_with_task_user", "x-pack-test-password", "minimal_with_task", false)
            .user("readonly_user", "x-pack-test-password", "readonly", false)
            .user("dest_only_user", "x-pack-test-password", "dest_only", false)
            .user("can_not_see_hidden_docs_user", "x-pack-test-password", "can_not_see_hidden_docs", false)
            .user("can_not_see_hidden_fields_user", "x-pack-test-password", "can_not_see_hidden_fields", false)
            .build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static Path httpCertificateAuthority;

    @BeforeClass
    public static void findTrustStore() throws Exception {
        final URL resource = ReindexWithSecurityClientYamlTestSuiteIT.class.getResource("/ssl/ca.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/ca.crt");
        }
        httpCertificateAuthority = PathUtils.get(resource.toURI());
    }

    @AfterClass
    public static void cleanupStatics() {
        httpCertificateAuthority = null;
    }

    @Override
    protected String getProtocol() {
        return "https";
    }

    /**
     * All tests run as a an administrative user but use <code>es-security-runas-user</code> to become a less privileged user.
     */
    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CERTIFICATE_AUTHORITIES, httpCertificateAuthority)
            .build();
    }

    public void testDeleteByQuery() throws IOException {
        createIndicesWithRandomAliases("test1", "test2", "test3");

        final Map<String, String> waitForCompletion = Map.of("wait_for_completion", "true");
        final String matchAllQuery = """
            {
                "query": {
                    "match_all": {}
                }
            }""";

        Response response = executeIgnoringConflicts(
            HttpPost.METHOD_NAME,
            "test1,test2/_delete_by_query",
            matchAllQuery,
            waitForCompletion
        );
        assertNotNull(response);

        response = executeIgnoringConflicts(HttpPost.METHOD_NAME, "test*/_delete_by_query", matchAllQuery, waitForCompletion);
        assertNotNull(response);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> execute(HttpPost.METHOD_NAME, "test1,index1/_delete_by_query", matchAllQuery, waitForCompletion)
        );
        assertThat(e.getMessage(), containsString("no such index [index1]"));
    }

    public void testUpdateByQuery() throws IOException {
        createIndicesWithRandomAliases("test1", "test2", "test3");

        Response response = executeIgnoringConflicts(HttpPost.METHOD_NAME, "test1,test2/_update_by_query", "", Map.of());
        assertNotNull(response);

        response = executeIgnoringConflicts(HttpPost.METHOD_NAME, "test*/_update_by_query", "", Map.of());
        assertNotNull(response);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> execute(HttpPost.METHOD_NAME, "test1,index1/_update_by_query", "", Map.of())
        );
        assertThat(e.getMessage(), containsString("no such index [index1]"));
    }

    public void testReindex() throws IOException {
        createIndicesWithRandomAliases("test1", "test2", "test3", "dest");

        final Map<String, String> waitForCompletion = Map.of("wait_for_completion", "true");
        Response response = execute(HttpPost.METHOD_NAME, "/_reindex", """
            {
               "source":{
                 "index": ["test1", "test2"]
               },
               "dest":{
                 "index":"dest"
               }
            }
            """, waitForCompletion);
        assertNotNull(response);

        response = execute(HttpPost.METHOD_NAME, "/_reindex", """
            {
               "source":{
                 "index": "test*"
               },
               "dest":{
                 "index":"dest"
               }
            }
            """, waitForCompletion);
        assertNotNull(response);

        ResponseException e = expectThrows(ResponseException.class, () -> { execute(HttpPost.METHOD_NAME, "/_reindex", """
            {
              "source":{
                "index": ["test1", "index1"]
              },
              "dest":{
                "index":"dest"
              }
            }
            """, waitForCompletion); });
        assertThat(e.getMessage(), containsString("no such index [index1]"));
    }

    /**
     * Creates the indices provided as argument, randomly associating them with aliases, indexes one dummy document per index
     * and refreshes the new indices
     */
    private void createIndicesWithRandomAliases(String... indices) throws IOException {
        for (String index : indices) {
            createIndex(index, Settings.EMPTY);
        }

        if (frequently()) {
            boolean aliasAdded = false;

            IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
            for (String index : indices) {
                if (frequently()) {
                    // one alias per index with prefix "alias-"
                    request.addAliasAction(AliasActions.add().index(index).alias("alias-" + index));
                    aliasAdded = true;
                }
            }
            // If we get to this point and we haven't added an alias to the request we need to add one
            // or the request will fail so use noAliasAdded to force adding the alias in this case
            if (aliasAdded == false || randomBoolean()) {
                // one alias pointing to all indices
                for (String index : indices) {
                    request.addAliasAction(AliasActions.add().index(index).alias("alias"));
                }
            }
            Request restRequest = new Request(HttpPost.METHOD_NAME, "/_aliases");
            XContentBuilder builder = jsonBuilder();
            request.toXContent(builder, null);
            restRequest.setEntity(new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON));
            Response restResponse = client().performRequest(restRequest);
            AcknowledgedResponse response = TestResponseParsers.parseAcknowledgedResponse(responseAsParser(restResponse));
            assertThat(response.isAcknowledged(), is(true));
        }

        for (String index : indices) {
            execute(HttpPost.METHOD_NAME, index + "/_doc", """
                {"field":"value"}
                """, Map.of());
        }
        refresh(client(), String.join(",", indices));
    }

    private static Response execute(String method, String endpoint, String jsonBody, Map<String, String> parameters) throws IOException {
        Request request = new Request(method, endpoint);
        request.setJsonEntity(jsonBody);
        if (parameters != null && parameters.size() > 0) {
            request.addParameters(parameters);
        }
        request.setOptions(RequestOptions.DEFAULT);
        return client().performRequest(request);
    }

    private static Response executeIgnoringConflicts(String method, String endpoint, String jsonBody, Map<String, String> parameters)
        throws IOException {
        try {
            return execute(method, endpoint, jsonBody, parameters);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 409) {
                return e.getResponse();
            } else {
                throw e;
            }
        }
    }
}
