/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDatafeedQueryValidator;
import org.junit.Before;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.security.cloud.CloudCredentialTestUtils.randomPersistedCloudCredential;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Drives {@link JobConfigProvider#validateDatafeedJob} end to end: job lookup, required-columns probe, source-time-field probe. */
public class JobConfigProviderTests extends ESTestCase {

    private static final String ESQL_QUERY = "FROM logs-* | STATS c = COUNT(*) BY time = BUCKET(@timestamp, 1h)";
    private static final Map<String, String> HEADERS = Map.of("header-name", "header-value");

    private Client client;
    private CloudCredentialManager credentialManager;
    private EsqlDatafeedQueryValidator queryValidator;

    @Before
    public void setUpMocks() throws IOException {
        client = mock(Client.class);
        credentialManager = mock(CloudCredentialManager.class);
        queryValidator = mock(EsqlDatafeedQueryValidator.class);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);
        GetResponse jobResponse = new GetResponse(
            new GetResult(".ml-config", Job.documentId("job"), 0, 1, 1, true, jobSource(), Collections.emptyMap(), Collections.emptyMap())
        );
        doAnswer(invocation -> {
            invocation.<ActionListener<GetResponse>>getArgument(1).onResponse(jobResponse);
            return null;
        }).when(client).get(any(), any());
    }

    private static BytesReference jobSource() throws IOException {
        Job.Builder builder = new Job.Builder("job");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(new Detector.Builder("count", null).build())
        );
        analysisConfig.setSummaryCountFieldName("c");
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(new DataDescription.Builder());
        Job job = builder.build(new Date());
        return BytesReference.bytes(
            job.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(JobConfigProvider.TO_XCONTENT_PARAMS))
        );
    }

    private JobConfigProvider newProvider(boolean crossProjectEnabled) {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", crossProjectEnabled).build();
        return new JobConfigProvider(
            client,
            NamedXContentRegistry.EMPTY,
            () -> credentialManager,
            new CrossProjectModeDecider(settings),
            queryValidator
        );
    }

    private static DatafeedConfig.Builder newEsqlDatafeed() {
        return new DatafeedConfig.Builder("df", "job").setEsqlQuery(ESQL_QUERY)
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1));
    }

    /** Completes the {@code validateQuery} probe with {@code failure}, or successfully when it is null. */
    private void stubColumnsProbe(Exception failure) {
        doAnswer(invocation -> {
            complete(invocation.getArgument(6), failure);
            return null;
        }).when(queryValidator).validateQuery(any(), any(), anyString(), any(), anyString(), any(), any(), any());
    }

    private void stubSourceTimeFieldProbe(Exception failure) {
        doAnswer(invocation -> {
            complete(invocation.getArgument(5), failure);
            return null;
        }).when(queryValidator).validateSourceTimeField(any(), any(), anyString(), any(), anyString(), any(), any());
    }

    private static void complete(ActionListener<Boolean> listener, Exception failure) {
        if (failure != null) {
            listener.onFailure(failure);
        } else {
            listener.onResponse(Boolean.TRUE);
        }
    }

    private void stubWrapClientWithoutCredential(Client wrapped) {
        when(credentialManager.wrapClient(same(client), nullable(PersistedCloudCredential.class))).thenReturn(wrapped);
    }

    /** Runs validation and returns the failure, asserting that the listener completes exactly once. */
    private Exception validateExpectingFailure(JobConfigProvider provider, DatafeedConfig config) {
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        provider.validateDatafeedJob(config, HEADERS, ActionListener.wrap(r -> fail("expected failure"), e -> {
            calls.incrementAndGet();
            failure.set(e);
        }));
        assertThat(calls.get(), equalTo(1));
        return failure.get();
    }

    private void validateExpectingSuccess(JobConfigProvider provider, DatafeedConfig config) {
        AtomicInteger calls = new AtomicInteger();
        provider.validateDatafeedJob(config, HEADERS, ActionTestUtils.assertNoFailureListener(ok -> {
            assertThat(ok, equalTo(Boolean.TRUE));
            calls.incrementAndGet();
        }));
        assertThat(calls.get(), equalTo(1));
    }

    public void testValidateDatafeedJobWhenDatafeedIsDslShouldNotProbe() {
        validateExpectingSuccess(newProvider(false), new DatafeedConfig.Builder("df", "job").setIndices(List.of("logs-*")).build());

        verifyNoInteractions(queryValidator, credentialManager);
    }

    public void testValidateDatafeedJobWhenRequiredColumnsProbeFailsShouldNotRunSourceTimeFieldProbe() {
        ElasticsearchStatusException columnsFailure = new ElasticsearchStatusException("missing time column", RestStatus.BAD_REQUEST);
        stubColumnsProbe(columnsFailure);
        stubWrapClientWithoutCredential(client);

        assertThat(validateExpectingFailure(newProvider(false), newEsqlDatafeed().build()), sameInstance(columnsFailure));

        verify(queryValidator, never()).validateSourceTimeField(any(), any(), anyString(), any(), anyString(), any(), any());
    }

    public void testValidateDatafeedJobWhenSourceTimeFieldProbeFailsShouldFailValidation() {
        ElasticsearchStatusException sourceTimeFailure = new ElasticsearchStatusException("not a date", RestStatus.BAD_REQUEST);
        stubColumnsProbe(null);
        stubSourceTimeFieldProbe(sourceTimeFailure);
        stubWrapClientWithoutCredential(client);

        assertThat(validateExpectingFailure(newProvider(false), newEsqlDatafeed().build()), sameInstance(sourceTimeFailure));
    }

    public void testValidateDatafeedJobWhenBothProbesSucceedShouldProbeColumnsFirstWithGivenHeaders() {
        stubColumnsProbe(null);
        stubSourceTimeFieldProbe(null);
        stubWrapClientWithoutCredential(client);

        validateExpectingSuccess(newProvider(false), newEsqlDatafeed().setProjectRouting("_alias:prod-*").build());

        // cross-project search is off, so the project routing is not applied to the probes
        InOrder inOrder = inOrder(queryValidator);
        inOrder.verify(queryValidator)
            .validateQuery(same(client), eq(HEADERS), eq(ESQL_QUERY), eq(null), eq("time"), eq("c"), any(), eq("df"));
        inOrder.verify(queryValidator)
            .validateSourceTimeField(same(client), eq(HEADERS), eq(ESQL_QUERY), eq(null), eq("@timestamp"), any(), eq("df"));
        inOrder.verifyNoMoreInteractions();
    }

    /**
     * The probes run after any credential mint, so they must search as the datafeed will at runtime: through a client wrapped
     * with the datafeed's own credential and with the project routing that cross-project mode keeps.
     */
    public void testValidateDatafeedJobWhenCrossProjectEnabledShouldProbeWithDatafeedCredentialAndRouting() {
        assumeTrue("feature under test must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
        PersistedCloudCredential credential = randomPersistedCloudCredential("minted-key-id");
        Client wrappedClient = mock(Client.class);
        when(credentialManager.wrapClient(same(client), same(credential))).thenReturn(wrappedClient);
        stubColumnsProbe(null);
        stubSourceTimeFieldProbe(null);

        validateExpectingSuccess(
            newProvider(true),
            newEsqlDatafeed().setProjectRouting("_alias:prod-*").setCloudInternalCredential(credential).build()
        );

        verify(queryValidator).validateQuery(
            same(wrappedClient),
            eq(HEADERS),
            eq(ESQL_QUERY),
            eq("_alias:prod-*"),
            eq("time"),
            eq("c"),
            any(),
            eq("df")
        );
        verify(queryValidator).validateSourceTimeField(
            same(wrappedClient),
            eq(HEADERS),
            eq(ESQL_QUERY),
            eq("_alias:prod-*"),
            eq("@timestamp"),
            any(),
            eq("df")
        );
    }
}
