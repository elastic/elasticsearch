/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportDeleteFilterActionTests extends ESTestCase {

    public void testDoExecute_ClusterStateJobUsesFilter() {

        Job.Builder builder = creatJobUsingFilter("job-using-filter", "filter-foo");
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(builder.build(), false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        TransportDeleteFilterAction action = new TransportDeleteFilterAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(TransportService.class), mock(ActionFilters.class), mock(IndexNameExpressionResolver.class),
                mock(Client.class), clusterService, mock(JobConfigProvider.class));

        DeleteFilterAction.Request request = new DeleteFilterAction.Request("filter-foo");
        AtomicReference<Exception> requestException = new AtomicReference<>();
        action.doExecute(request, ActionListener.wrap(
                response -> fail("response was not expected"),
                requestException::set
        ));

        assertThat(requestException.get(), instanceOf(ElasticsearchStatusException.class));
        assertEquals("Cannot delete filter [filter-foo] currently used by jobs [job-using-filter]", requestException.get().getMessage());
    }

    private Job.Builder creatJobUsingFilter(String jobId, String filterId) {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", filterId)).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(Collections.singletonList(
                detectorReferencingFilter.build()));

        Job.Builder builder = new Job.Builder(jobId);
        builder.setAnalysisConfig(filterAnalysisConfig);
        builder.setDataDescription(new DataDescription.Builder());
        builder.setCreateTime(new Date());
        return builder;
    }
}


