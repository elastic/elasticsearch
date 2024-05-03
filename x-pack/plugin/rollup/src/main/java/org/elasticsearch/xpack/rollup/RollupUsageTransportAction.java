/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class RollupUsageTransportAction extends XPackUsageFeatureTransportAction {

    private static final XContentParserConfiguration PARSER_CONFIGURATION = XContentParserConfiguration.EMPTY.withFiltering(
        Set.of("_meta._rollup"),
        null,
        false
    );

    @Inject
    public RollupUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            XPackUsageFeatureAction.ROLLUP.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        int numberOfRollupIndices = 0;
        for (var imd : state.metadata()) {
            try (var parser = XContentHelper.createParser(PARSER_CONFIGURATION, imd.mapping().source().compressedReference())) {
                if ("_rollup".equals(parser.currentName())) {
                    var rollupConfig = parser.map();
                    if (rollupConfig != null) {
                        numberOfRollupIndices++;
                    }
                }
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
        }

        int numberOfRollupJobs = 0;
        PersistentTasksCustomMetadata persistentTasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (persistentTasks != null) {
            numberOfRollupJobs = persistentTasks.findTasks(RollupJob.NAME, Predicates.always()).size();
        }
        RollupFeatureSetUsage usage = new RollupFeatureSetUsage(numberOfRollupIndices, numberOfRollupJobs);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
