/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public abstract class AbstractChallengeRestTest extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    private final String baselineDataStreamName;
    private final String contenderDataStreamName;
    private final String baselineTemplateName;
    private final String contenderTemplateName;

    private final int baselineTemplatePriority;
    private final int contenderTemplatePriority;
    private XContentBuilder baselineMappings;
    private XContentBuilder contenderMappings;
    private Settings.Builder baselineSettings;
    private Settings.Builder contenderSettings;
    protected RestClient client;

    @ClassRule()
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .user(USER, PASS)
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public AbstractChallengeRestTest(
        final String baselineDataStreamName,
        final String contenderDataStreamName,
        final String baselineTemplateName,
        final String contenderTemplateName,
        int baselineTemplatePriority,
        int contenderTemplatePriority
    ) {
        this.baselineDataStreamName = baselineDataStreamName;
        this.contenderDataStreamName = contenderDataStreamName;
        this.baselineTemplateName = baselineTemplateName;
        this.contenderTemplateName = contenderTemplateName;
        this.baselineTemplatePriority = baselineTemplatePriority;
        this.contenderTemplatePriority = contenderTemplatePriority;
    }

    @Before
    public void beforeTest() throws Exception {
        beforeStart();
        client = client();
        this.baselineMappings = createBaselineMappings();
        this.contenderMappings = createContenderMappings();
        this.baselineSettings = createBaselineSettings();
        this.contenderSettings = createContenderSettings();
        createTemplates();
        createDataStreams();
        beforeEnd();
    }

    @After
    public void afterTest() throws Exception {
        afterStart();
        deleteDataStreams();
        deleteTemplates();
        afterEnd();
    }

    public void beforeStart() throws Exception {}

    public void beforeEnd() throws Exception {};

    public void afterStart() throws Exception {}

    public void afterEnd() throws Exception {}

    private void createTemplates() throws IOException {
        final Response createBaselineTemplateResponse = createTemplates(
            getBaselineTemplateName(),
            getBaselineDataStreamName() + "*",
            baselineSettings,
            baselineMappings,
            getBaselineTemplatePriority()
        );
        assert createBaselineTemplateResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();

        final Response createContenderTemplateResponse = createTemplates(
            getContenderTemplateName(),
            getContenderDataStreamName() + "*",
            contenderSettings,
            contenderMappings,
            getContenderTemplatePriority()
        );
        assert createContenderTemplateResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private void createDataStreams() throws IOException {
        final Response craeteBaselineDataStreamResponse = client.performRequest(
            new Request("PUT", "_data_stream/" + getBaselineDataStreamName())
        );
        assert craeteBaselineDataStreamResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();

        final Response createContenderDataStreamResponse = client.performRequest(
            new Request("PUT", "_data_stream/" + getContenderDataStreamName())
        );
        assert createContenderDataStreamResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private Response createTemplates(
        final String templateName,
        final String pattern,
        final Settings.Builder settings,
        final XContentBuilder mappings,
        int priority
    ) throws IOException {
        final String template = """
            {
              "index_patterns": [ "%s" ],
              "template": {
                "settings":%s,
                "mappings": %s
              },
              "data_stream": {},
              "priority": %d
            }
            """;
        final Request request = new Request("PUT", "/_index_template/" + templateName);
        final String jsonSettings = settings.build().toString();
        final String jsonMappings = Strings.toString(mappings);
        request.setJsonEntity(Strings.format(template, pattern, jsonSettings, jsonMappings, priority));
        return client.performRequest(request);
    }

    private void deleteDataStreams() throws IOException {
        final Response deleteBaselineDataStream = client.performRequest(
            new Request("DELETE", "/_data_stream/" + getBaselineDataStreamName())
        );
        assert deleteBaselineDataStream.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();

        final Response deleteContenderDataStream = client.performRequest(
            new Request("DELETE", "/_data_stream/" + getContenderDataStreamName())
        );
        assert deleteContenderDataStream.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private void deleteTemplates() throws IOException {
        final Response deleteBaselineTemplate = client.performRequest(
            new Request("DELETE", "/_index_template/" + getBaselineTemplateName())
        );
        assert deleteBaselineTemplate.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();

        final Response deleteContenderTemplate = client.performRequest(
            new Request("DELETE", "/_index_template/" + getContenderTemplateName())
        );
        assert deleteContenderTemplate.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private Settings.Builder createSettings(
        final CheckedConsumer<Settings.Builder, IOException> settingsConsumer,
        final CheckedConsumer<Settings.Builder, IOException> commonSettingsConsumer
    ) throws IOException {
        final Settings.Builder settings = Settings.builder();
        settingsConsumer.accept(settings);
        commonSettingsConsumer.accept(settings);
        return settings;
    }

    private Settings.Builder createBaselineSettings() throws IOException {
        return createSettings(this::baselineSettings, this::commonSettings);
    }

    private Settings.Builder createContenderSettings() throws IOException {
        return createSettings(this::contenderSettings, this::commonSettings);
    }

    private XContentBuilder createMappings(final CheckedConsumer<XContentBuilder, IOException> builderConsumer) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builderConsumer.accept(builder);
        return builder;
    }

    private XContentBuilder createBaselineMappings() throws IOException {
        return createMappings(this::baselineMappings);
    }

    private XContentBuilder createContenderMappings() throws IOException {
        return createMappings(this::contenderMappings);
    }

    public abstract void baselineMappings(XContentBuilder builder) throws IOException;

    public abstract void contenderMappings(XContentBuilder builder) throws IOException;

    public abstract void baselineSettings(Settings.Builder builder);

    public abstract void contenderSettings(Settings.Builder builder);

    public void commonSettings(Settings.Builder builder) {}

    public abstract void indexDocuments(
        CheckedSupplier<List<XContentBuilder>, IOException> baselineSupplier,
        CheckedSupplier<List<XContentBuilder>, IOException> contenderSupplier
    ) throws IOException;

    public Response queryBaseline(final SearchSourceBuilder search) throws IOException {
        return query(search, this::getBaselineDataStreamName);
    }

    public Response queryContender(final SearchSourceBuilder search) throws IOException {
        return query(search, this::getContenderDataStreamName);
    }

    private Response query(final SearchSourceBuilder search, final Supplier<String> dataStreamNameSupplier) throws IOException {
        final Request request = new Request("GET", "/" + dataStreamNameSupplier.get() + "/_search");
        request.setJsonEntity(Strings.toString(search));
        return client.performRequest(request);
    }

    public Response esqlBaseline(final String query) throws IOException {
        return esql(query, this::getBaselineDataStreamName);
    }

    public Response esqlContender(final String query) throws IOException {
        return esql(query, this::getContenderDataStreamName);
    }

    private Response esql(final String query, final Supplier<String> dataStreamNameSupplier) throws IOException {
        final Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query.replace("$index", dataStreamNameSupplier.get()) + "\"}");
        return client.performRequest(request);
    }

    public Response fieldCapsBaseline() throws IOException {
        return fieldCaps(this::getBaselineDataStreamName);
    }

    public Response fieldCapsContender() throws IOException {
        return fieldCaps(this::getContenderDataStreamName);
    }

    private Response fieldCaps(final Supplier<String> dataStreamNameSupplier) throws IOException {
        final Request request = new Request("GET", "/" + dataStreamNameSupplier.get() + "/_field_caps?fields=*");
        return client.performRequest(request);
    }

    public String getBaselineDataStreamName() {
        return baselineDataStreamName;
    }

    public int getBaselineTemplatePriority() {
        return baselineTemplatePriority;
    }

    public int getContenderTemplatePriority() {
        return contenderTemplatePriority;
    }

    public String getContenderDataStreamName() {
        return contenderDataStreamName;
    }

    public String getBaselineTemplateName() {
        return baselineTemplateName;
    }

    public String getContenderTemplateName() {
        return contenderTemplateName;
    }

    public XContentBuilder getBaselineMappings() {
        return baselineMappings;
    }

    public XContentBuilder getContenderMappings() {
        return contenderMappings;
    }

    public Settings.Builder getBaselineSettings() {
        return baselineSettings;
    }

    public Settings.Builder getContenderSettings() {
        return contenderSettings;
    }
}
