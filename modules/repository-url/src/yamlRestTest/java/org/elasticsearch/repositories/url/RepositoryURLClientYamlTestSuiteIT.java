/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.url;

import fixture.url.URLFixture;
import io.netty.handler.codec.http.HttpMethod;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class RepositoryURLClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public static final URLFixture urlFixture = new URLFixture();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-url")
        .setting("path.repo", urlFixture::getRepositoryDir)
        .setting("repositories.url.allowed_urls", () -> "http://snapshot.test*, " + urlFixture.getAddress() + "," + urlFixture.getFtpUrl())
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(urlFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public RepositoryURLClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    /**
     * This method registers 4 snapshot/restore repositories:
     * <ol>
     * <li>{@code repository-fs}: this FS repository is used to create snapshots.</li>
     * <li>{@code repository-url-http}: this URL repository is used to restore snapshots created using the previous repository. It uses
     *     the {@link URLFixture} to restore snapshots over HTTP.</li>
     * <li>{@code repository-url-file}: similar as the previous repository but using the {@code file://} scheme instead of
     *     {@code http://}.</li>
     * <li>{@code repository-url-ftp}: similar as the previous repository but using the {@code ftp://} scheme instead of
     *     {@code http://}.</li>
     * </ol>
     **/
    @Before
    public void registerRepositories() throws IOException {
        Request clusterSettingsRequest = new Request("GET", "/_cluster/settings");
        clusterSettingsRequest.addParameter("include_defaults", "true");
        clusterSettingsRequest.addParameter("filter_path", "defaults.path.repo,defaults.repositories.url.allowed_urls");
        Response clusterSettingsResponse = client().performRequest(clusterSettingsRequest);
        Map<String, Object> clusterSettings = entityAsMap(clusterSettingsResponse);

        @SuppressWarnings("unchecked")
        List<String> pathRepos = (List<String>) XContentMapValues.extractValue("defaults.path.repo", clusterSettings);
        assertThat(pathRepos, notNullValue());
        assertThat(pathRepos, hasSize(1));

        final String pathRepo = pathRepos.get(0);
        final URI pathRepoUri = PathUtils.get(pathRepo).toUri().normalize();

        createRepository("repository-fs", FsRepository.TYPE, b -> b.put("location", pathRepo));
        createUrlRepository("file", pathRepoUri.toString());
        createUrlRepository("http", urlFixture.getAddress());
        createUrlRepository("ftp", urlFixture.getFtpUrl());
    }

    private static void createUrlRepository(final String nameSuffix, final String url) throws IOException {
        assertThat(url, startsWith(nameSuffix + "://"));
        createRepository("repository-url-" + nameSuffix, URLRepository.TYPE, b -> b.put("url", url));
    }

    private static void createRepository(final String name, final String type, final UnaryOperator<Settings.Builder> settings)
        throws IOException {
        assertOK(client().performRequest(ESRestTestCase.newXContentRequest(HttpMethod.PUT, "/_snapshot/" + name, (builder, params) -> {
            builder.field("type", type);
            builder.startObject("settings");
            settings.apply(Settings.builder()).build().toXContent(builder, params);
            builder.endObject();
            return builder;
        })));
    }
}
