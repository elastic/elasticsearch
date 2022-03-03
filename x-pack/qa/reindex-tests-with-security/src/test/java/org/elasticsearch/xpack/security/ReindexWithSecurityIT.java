/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("removal")
public class ReindexWithSecurityIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

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

        RestHighLevelClient restClient = new TestRestHighLevelClient();
        BulkByScrollResponse response = restClient.deleteByQuery(
            new DeleteByQueryRequest().setQuery(QueryBuilders.matchAllQuery()).indices("test1", "test2"),
            RequestOptions.DEFAULT
        );
        assertNotNull(response);

        response = restClient.deleteByQuery(
            new DeleteByQueryRequest().setQuery(QueryBuilders.matchAllQuery()).indices("test*"),
            RequestOptions.DEFAULT
        );
        assertNotNull(response);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> restClient.deleteByQuery(
                new DeleteByQueryRequest().setQuery(QueryBuilders.matchAllQuery()).indices("test1", "index1"),
                RequestOptions.DEFAULT
            )
        );
        assertThat(e.getMessage(), containsString("no such index [index1]"));
    }

    public void testUpdateByQuery() throws IOException {
        createIndicesWithRandomAliases("test1", "test2", "test3");

        RestHighLevelClient restClient = new TestRestHighLevelClient();
        BulkByScrollResponse response = restClient.updateByQuery(
            (UpdateByQueryRequest) new UpdateByQueryRequest().indices("test1", "test2"),
            RequestOptions.DEFAULT
        );
        assertNotNull(response);

        response = restClient.updateByQuery((UpdateByQueryRequest) new UpdateByQueryRequest().indices("test*"), RequestOptions.DEFAULT);
        assertNotNull(response);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> restClient.updateByQuery(
                (UpdateByQueryRequest) new UpdateByQueryRequest().indices("test1", "index1"),
                RequestOptions.DEFAULT
            )
        );
        assertThat(e.getMessage(), containsString("no such index [index1]"));
    }

    public void testReindex() throws IOException {
        createIndicesWithRandomAliases("test1", "test2", "test3", "dest");

        RestHighLevelClient restClient = new TestRestHighLevelClient();
        BulkByScrollResponse response = restClient.reindex(
            new ReindexRequest().setSourceIndices("test1", "test2").setDestIndex("dest"),
            RequestOptions.DEFAULT
        );
        assertNotNull(response);

        response = restClient.reindex(new ReindexRequest().setSourceIndices("test*").setDestIndex("dest"), RequestOptions.DEFAULT);
        assertNotNull(response);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> restClient.reindex(new ReindexRequest().setSourceIndices("test1", "index1").setDestIndex("dest"), RequestOptions.DEFAULT)
        );
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

        RestHighLevelClient restClient = new TestRestHighLevelClient();
        if (frequently()) {
            boolean aliasAdded = false;

            IndicesAliasesRequest request = new IndicesAliasesRequest();
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
            AcknowledgedResponse response = restClient.indices().updateAliases(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }

        for (String index : indices) {
            restClient.index(new IndexRequest(index).source("field", "value"), RequestOptions.DEFAULT);
        }
        restClient.indices().refresh(new RefreshRequest(indices), RequestOptions.DEFAULT);
    }

    private class TestRestHighLevelClient extends RestHighLevelClient {
        TestRestHighLevelClient() {
            super(client(), restClient -> {}, Collections.emptyList());
        }
    }
}
