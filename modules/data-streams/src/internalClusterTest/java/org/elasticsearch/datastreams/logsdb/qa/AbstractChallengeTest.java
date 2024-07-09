/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * This class encapsulates the logic for running tests using two data streams to compare results
 * from indexing and querying operations. The `oracle` data stream serves as the source of truth,
 * while the `challenge` data stream is tested against the oracle. Comparing operation results
 * on both data streams ensures that the challenge data stream behaves as expected relative to the
 * oracle data stream.
 */
public abstract class AbstractChallengeTest extends ESSingleNodeTestCase {
    private final String oracleDataStreamName;
    private final String challengeDataStreamName;
    private final String oracleTemplateName;
    private final String challengeTemplateName;

    // NOTE: this is a single node test, having more shards or replicas results in a yellow cluster
    private static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    private static final int DEFAULT_NUMBER_OF_REPLICAS = 1;
    private XContentBuilder oracleMappings;
    private XContentBuilder challengeMappings;
    private Settings.Builder oracleSettings;
    private Settings.Builder challengeSettings;

    public AbstractChallengeTest(
        final String oracleDataStreamName,
        final String challengeDataStreamName,
        final String oracleTemplateName,
        final String challengeTemplateName
    ) {
        this.oracleDataStreamName = oracleDataStreamName;
        this.challengeDataStreamName = challengeDataStreamName;
        this.oracleTemplateName = oracleTemplateName;
        this.challengeTemplateName = challengeTemplateName;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(DataStreamsPlugin.class);
    }

    @Before
    public void setup() throws Exception {
        super.setUp();
        this.oracleMappings = createOracleMappings();
        this.challengeMappings = createChallengeMappings();
        this.oracleSettings = createOracleSettings();
        this.challengeSettings = createChallengeSettings();
        createTemplates();
        createDataStreams();
    }

    private void createTemplates() throws IOException {
        final AcknowledgedResponse putOracleTemplateResponse = createTemplates(
            getOracleTemplateName(),
            getOracleDataStreamName() + "*",
            oracleSettings,
            oracleMappings
        );
        assert putOracleTemplateResponse.isAcknowledged() : "error while creating template [" + getOracleTemplateName() + "]";

        final AcknowledgedResponse putChallengeTemplateResponse = createTemplates(
            getChallengeTemplateName(),
            getChallengeDataStreamName() + "*",
            challengeSettings,
            challengeMappings
        );
        assert putChallengeTemplateResponse.isAcknowledged()
            : "error while creating challenge template [" + getChallengeTemplateName() + "]";
    }

    private void createDataStreams() {
        assert client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(getOracleDataStreamName()))
            .actionGet()
            .isAcknowledged() : "error while creating data stream [" + getOracleDataStreamName() + "]";
        assert client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(getChallengeDataStreamName()))
            .actionGet()
            .isAcknowledged() : "error while creating data stream [" + getChallengeDataStreamName() + "]";
    }

    private AcknowledgedResponse createTemplates(
        final String templateName,
        final String pattern,
        final Settings.Builder settings,
        final XContentBuilder mappings
    ) throws IOException {
        final TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            templateName
        );
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(pattern))
                .template(new Template(settings.build(), new CompressedXContent(Strings.toString(mappings)), null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        return client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
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

    private BulkResponse indexDocuments(
        final String dataStreamName,
        final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier
    ) throws IOException {
        final BulkRequest bulkRequest = new BulkRequest();
        for (var document : documentsSupplier.get()) {
            final IndexRequest indexRequest = new IndexRequest().index(dataStreamName).source(document);
            indexRequest.opType(DocWriteRequest.OpType.CREATE);
            bulkRequest.add(indexRequest);
        }
        return client().bulk(bulkRequest).actionGet();
    }

    public BulkResponse indexOracleDocuments(final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier)
        throws IOException {
        return indexDocuments(getOracleDataStreamName(), documentsSupplier);
    }

    public BulkResponse indexChallengeDocuments(final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier)
        throws IOException {
        return indexDocuments(getChallengeDataStreamName(), documentsSupplier);
    }

    public Tuple<BulkResponse, BulkResponse> indexDocuments(
        final CheckedSupplier<List<XContentBuilder>, IOException> oracleSupplier,
        final CheckedSupplier<List<XContentBuilder>, IOException> challengeSupplier
    ) throws IOException {
        return new Tuple<>(indexOracleDocuments(oracleSupplier), indexChallengeDocuments(challengeSupplier));
    }

    public SearchResponse queryOracle(final SearchSourceBuilder search) {
        return query(search, this::getOracleDataStreamName);
    }

    public SearchResponse queryChallenge(final SearchSourceBuilder search) {
        return query(search, this::getChallengeDataStreamName);
    }

    private SearchResponse query(final SearchSourceBuilder search, final Supplier<String> dataStreamNameSupplier) {
        return client().search(new SearchRequest(dataStreamNameSupplier.get()).source(search)).actionGet();
    }

    public BroadcastResponse refreshOracle() {
        return refresh(this::getOracleDataStreamName);
    }

    public BroadcastResponse refreshChallenge() {
        return refresh(this::getChallengeDataStreamName);
    }

    private BroadcastResponse refresh(final Supplier<String> dataStreamName) {
        return client().admin().indices().refresh(new RefreshRequest().indices(dataStreamName.get())).actionGet();
    }

    public Tuple<BroadcastResponse, BroadcastResponse> refresh() {
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
