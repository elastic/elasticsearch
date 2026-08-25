/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assume.assumeTrue;

public class DatafeedCrossProjectIT extends MlSingleNodeTestCase {

    private DatafeedConfigProvider datafeedConfigProvider;
    private String dummyAuthenticationHeader;

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("serverless.cross_project.enabled", true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(CpsPlugin.class)).toList();
    }

    public static class CpsPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(Setting.boolSetting("serverless.cross_project.enabled", false, Setting.Property.NodeScope));
        }
    }

    @Before
    public void createComponents() throws Exception {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
        datafeedConfigProvider = new DatafeedConfigProvider(client(), xContentRegistry(), getInstanceFromNode(ClusterService.class));
        waitForMlTemplates();
        dummyAuthenticationHeader = Authentication.newRealmAuthentication(
            new org.elasticsearch.xpack.core.security.user.User("dummy"),
            new Authentication.RealmRef("name", "type", "node")
        ).encode();
    }

    public void testGetDatafeedWithProjectRouting() throws Exception {
        String datafeedId = "datafeed_with_project_routing";
        String jobId = "job_for_datafeed_project_routing";
        String expectedProjectRouting = "_alias:prod-*";

        // Create datafeed with project_routing
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedBuilder.setIndices(List.of("logs-*"));
        datafeedBuilder.setProjectRouting(expectedProjectRouting);

        AtomicReference<Tuple<DatafeedConfig, DocWriteResponse>> putResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            actionListener -> datafeedConfigProvider.putDatafeedConfig(datafeedBuilder.build(), createSecurityHeader(), actionListener),
            putResponseHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertThat(putResponseHolder.get().v2().status(), equalTo(RestStatus.CREATED));

        // Get datafeed and verify project_routing is returned
        AtomicReference<DatafeedConfig.Builder> getResponseHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.getDatafeedConfig(datafeedId, null, actionListener),
            getResponseHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());

        DatafeedConfig retrievedDatafeed = getResponseHolder.get().build();
        assertThat(retrievedDatafeed.getProjectRouting(), equalTo(expectedProjectRouting));
    }

    public void testStartWithoutCredentialShouldProcessOriginData() throws Exception {
        String jobId = "job_no_credential";
        String datafeedId = "datafeed_no_credential";

        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();

        client().execute(
            PutJobAction.INSTANCE,
            new PutJobAction.Request(
                new Job.Builder().setId(jobId)
                    .setAnalysisLimits(new AnalysisLimits(ByteSizeValue.ofMb(2).getMb(), null))
                    .setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("count", null).build())))
                    .setDataDescription(new DataDescription.Builder().setTimeFormat(DataDescription.EPOCH_MS))
            )
        ).actionGet();

        DatafeedConfig config = BaseMlIntegTestCase.createDatafeed(datafeedId, jobId, Collections.singletonList("data"));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(config)).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertThat(statsResponse.getResponse().results().get(0).getState(), equalTo(JobState.OPENED));
        });

        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000L;
        BaseMlIntegTestCase.indexDocs(client(), logger, "data", 100, weekAgo, now);

        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L)).actionGet();

        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(datafeedId)
            ).actionGet();
            assertThat(statsResponse.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STARTED));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertThat(statsResponse.getResponse().results().get(0).getDataCounts().getInputRecordCount(), greaterThan(0L));
        });

        client().execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId)).actionGet();
        client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId)).actionGet();
    }

    private Map<String, String> createSecurityHeader() {
        Map<String, String> headers = new HashMap<>();
        // Only security headers are updated, grab the first one
        String securityHeader = ClientHelper.SECURITY_HEADER_FILTERS.iterator().next();
        if (Set.of(
            AuthenticationField.AUTHENTICATION_KEY,
            org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication.THREAD_CTX_KEY
        ).contains(securityHeader)) {
            headers.put(securityHeader, dummyAuthenticationHeader);
        } else {
            headers.put(securityHeader, "SECURITY_");
        }
        return headers;
    }
}
