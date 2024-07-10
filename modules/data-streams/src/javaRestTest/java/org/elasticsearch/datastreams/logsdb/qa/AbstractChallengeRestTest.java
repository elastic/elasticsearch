/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
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
    private final String oracleDataStreamName;
    private final String challengeDataStreamName;
    private final String oracleTemplateName;
    private final String challengeTemplateName;

    private final int oracleTemplatePriority;
    private final int challengeTemplatePriority;

    // NOTE: this is a single node test, having more shards or replicas results in a yellow cluster
    private static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    private static final int DEFAULT_NUMBER_OF_REPLICAS = 1;
    private XContentBuilder oracleMappings;
    private XContentBuilder challengeMappings;
    private Settings.Builder oracleSettings;
    private Settings.Builder challengeSettings;
    private RestClient client;

    @ClassRule()
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public AbstractChallengeRestTest(
        final String oracleDataStreamName,
        final String challengeDataStreamName,
        final String oracleTemplateName,
        final String challengeTemplateName,
        int oracleTemplatePriority,
        int challengeTemplatePriority
    ) {
        this.oracleDataStreamName = oracleDataStreamName;
        this.challengeDataStreamName = challengeDataStreamName;
        this.oracleTemplateName = oracleTemplateName;
        this.challengeTemplateName = challengeTemplateName;
        this.oracleTemplatePriority = oracleTemplatePriority;
        this.challengeTemplatePriority = challengeTemplatePriority;
    }

    @Before
    public void beforeTest() throws Exception {
        beforeStart();
        client = client();
        this.oracleMappings = createOracleMappings();
        this.challengeMappings = createChallengeMappings();
        this.oracleSettings = createOracleSettings();
        this.challengeSettings = createChallengeSettings();
        createTemplates();
        createDataStreams();
        beforeEnd();
    }

    @After
    public void afterTest() throws Exception {
        afterStart();
        deleteTemplates();
        deleteDataStreams();
        afterEnd();
    }

    public void beforeStart() throws Exception {}

    public void beforeEnd() throws Exception {

    };

    public void afterStart() throws Exception {

    }

    public void afterEnd() throws Exception {

    }

    private void createTemplates() throws IOException {
        final Response createOracleTemplateResponse = createTemplates(
            getOracleTemplateName(),
            getOracleDataStreamName() + "*",
            oracleSettings,
            oracleMappings,
            getOracleTemplatePriority()
        );
        assert createOracleTemplateResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();

        final Response createChallengeTemplateResponse = createTemplates(
            getChallengeTemplateName(),
            getChallengeDataStreamName() + "*",
            challengeSettings,
            challengeMappings,
            getChallengeTemplatePriority()
        );
        assert createChallengeTemplateResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private void createDataStreams() throws IOException {
        final Response craeteOracleDataStreamResponse = client.performRequest(
            new Request("PUT", "_data_stream/" + getOracleDataStreamName())
        );
        assert craeteOracleDataStreamResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();

        final Response createChallengeDataStreamResponse = client.performRequest(
            new Request("PUT", "_data_stream/" + getChallengeDataStreamName())
        );
        assert createChallengeDataStreamResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
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
              "index_patterns": [ \"%s\" ],
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

    private void deleteTemplates() throws IOException {
        final Response deleteOracleDataStream = client.performRequest(new Request("DELETE", "/_data_stream/" + getOracleDataStreamName()));
        assert deleteOracleDataStream.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();

        final Response deleteChallengeDataStream = client.performRequest(
            new Request("DELETE", "/_data_stream/" + getChallengeDataStreamName())
        );
        assert deleteChallengeDataStream.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private void deleteDataStreams() throws IOException {
        final Response deleteOracleTemplate = client.performRequest(new Request("DELETE", "/_index_template/" + getOracleTemplateName()));
        assert deleteOracleTemplate.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();

        final Response deleteChallengeTemplate = client.performRequest(
            new Request("DELETE", "/_index_template/" + getChallengeTemplateName())
        );
        assert deleteChallengeTemplate.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private Settings.Builder createSettings(final CheckedConsumer<Settings.Builder, IOException> settingsConsumer) throws IOException {
        final Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", numberOfShards())
            .put("index.number_of_replicas", numberOfReplicas());
        settingsConsumer.accept(settings);
        return settings;
    }

    private Settings.Builder createOracleSettings() throws IOException {
        return createSettings(this::oracleSettings);
    }

    private Settings.Builder createChallengeSettings() throws IOException {
        return createSettings(this::challengeSettings);
    }

    private XContentBuilder createMappings(final CheckedConsumer<XContentBuilder, IOException> builderConsumer) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builderConsumer.accept(builder);
        builder.endObject();
        return builder;
    }

    private XContentBuilder createOracleMappings() throws IOException {
        return createMappings(this::oracleMappings);
    }

    private XContentBuilder createChallengeMappings() throws IOException {
        return createMappings(this::challengeMappings);
    }

    public abstract void oracleMappings(XContentBuilder builder) throws IOException;

    public abstract void challengeMappings(XContentBuilder builder) throws IOException;

    public void oracleSettings(Settings.Builder builder) {}

    public void challengeSettings(Settings.Builder builder) {}

    private Response indexDocuments(
        final String dataStreamName,
        final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier
    ) throws IOException {
        final StringBuilder sb = new StringBuilder();
        for (var document : documentsSupplier.get()) {
            sb.append("{ \"create\": {} }").append("\n");
            sb.append(Strings.toString(document)).append("\n");
        }
        var request = new Request("POST", "/" + dataStreamName + "/_bulk");
        request.setJsonEntity(sb.toString());
        request.addParameter("refresh", "true");
        return client.performRequest(request);
    }

    public Response indexOracleDocuments(final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier) throws IOException {
        return indexDocuments(getOracleDataStreamName(), documentsSupplier);
    }

    public Response indexChallengeDocuments(final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier)
        throws IOException {
        return indexDocuments(getChallengeDataStreamName(), documentsSupplier);
    }

    public Tuple<Response, Response> indexDocuments(
        final CheckedSupplier<List<XContentBuilder>, IOException> oracleSupplier,
        final CheckedSupplier<List<XContentBuilder>, IOException> challengeSupplier
    ) throws IOException {
        return new Tuple<>(indexOracleDocuments(oracleSupplier), indexChallengeDocuments(challengeSupplier));
    }

    public Response queryOracle(final SearchSourceBuilder search) throws IOException {
        return query(search, this::getOracleDataStreamName);
    }

    public Response queryChallenge(final SearchSourceBuilder search) throws IOException {
        return query(search, this::getChallengeDataStreamName);
    }

    private Response query(final SearchSourceBuilder search, final Supplier<String> dataStreamNameSupplier) throws IOException {
        final Request request = new Request("GET", "/" + dataStreamNameSupplier.get() + "/_search");
        request.setJsonEntity(Strings.toString(search));
        return client.performRequest(request);
    }

    public Response refreshOracle() throws IOException {
        return refresh(this::getOracleDataStreamName);
    }

    public Response refreshChallenge() throws IOException {
        return refresh(this::getChallengeDataStreamName);
    }

    private Response refresh(final Supplier<String> dataStreamName) throws IOException {
        return client.performRequest(new Request("POST", "/" + dataStreamName + "/_refresh"));
    }

    public Tuple<Response, Response> refresh() throws IOException {
        return new Tuple<>(refreshOracle(), refreshChallenge());
    }

    protected int numberOfShards() {
        return DEFAULT_NUMBER_OF_SHARDS;
    }

    protected int numberOfReplicas() {
        return DEFAULT_NUMBER_OF_REPLICAS;
    }

    public String getOracleDataStreamName() {
        return oracleDataStreamName;
    }

    public int getOracleTemplatePriority() {
        return oracleTemplatePriority;
    }

    public int getChallengeTemplatePriority() {
        return challengeTemplatePriority;
    }

    public String getChallengeDataStreamName() {
        return challengeDataStreamName;
    }

    public String getOracleTemplateName() {
        return oracleTemplateName;
    }

    public String getChallengeTemplateName() {
        return challengeTemplateName;
    }

    public XContentBuilder getOracleMappings() {
        return oracleMappings;
    }

    public XContentBuilder getChallengeMappings() {
        return challengeMappings;
    }

    public Settings.Builder getOracleSettings() {
        return oracleSettings;
    }

    public Settings.Builder getChallengeSettings() {
        return challengeSettings;
    }
}
