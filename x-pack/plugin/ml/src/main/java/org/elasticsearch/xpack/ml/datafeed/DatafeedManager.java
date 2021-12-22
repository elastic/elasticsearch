/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

/**
 * Allows interactions with datafeeds. The managed interactions include:
 * <ul>
 * <li>creation</li>
 * <li>reading</li>
 * <li>deletion</li>
 * <li>updating</li>
 * </ul>
 */
public final class DatafeedManager {

    private static final Logger logger = LogManager.getLogger(DatafeedManager.class);

    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobConfigProvider jobConfigProvider;
    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final Settings settings;

    public DatafeedManager(
        DatafeedConfigProvider datafeedConfigProvider,
        JobConfigProvider jobConfigProvider,
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        Client client
    ) {
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.jobConfigProvider = jobConfigProvider;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.settings = settings;
    }

    public void putDatafeed(
        PutDatafeedAction.Request request,
        ClusterState state,
        XPackLicenseState licenseState,
        SecurityContext securityContext,
        ThreadPool threadPool,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        if (XPackSettings.SECURITY_ENABLED.get(settings)) {
            useSecondaryAuthIfAvailable(securityContext, () -> {
                final String[] indices = request.getDatafeed().getIndices().toArray(new String[0]);

                final String username = securityContext.getUser().principal();
                final HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
                privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
                privRequest.username(username);
                privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);

                final RoleDescriptor.IndicesPrivileges.Builder indicesPrivilegesBuilder = RoleDescriptor.IndicesPrivileges.builder()
                    .indices(indices);

                ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                    r -> handlePrivsResponse(username, request, r, state, threadPool, listener),
                    listener::onFailure
                );

                ActionListener<GetRollupIndexCapsAction.Response> getRollupIndexCapsActionHandler = ActionListener.wrap(response -> {
                    if (response.getJobs().isEmpty()) { // This means no rollup indexes are in the config
                        indicesPrivilegesBuilder.privileges(SearchAction.NAME);
                    } else {
                        indicesPrivilegesBuilder.privileges(SearchAction.NAME, RollupSearchAction.NAME);
                    }
                    privRequest.indexPrivileges(indicesPrivilegesBuilder.build());
                    client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                        indicesPrivilegesBuilder.privileges(SearchAction.NAME);
                        privRequest.indexPrivileges(indicesPrivilegesBuilder.build());
                        client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
                    } else {
                        listener.onFailure(e);
                    }
                });
                if (RemoteClusterLicenseChecker.containsRemoteIndex(request.getDatafeed().getIndices())) {
                    getRollupIndexCapsActionHandler.onResponse(new GetRollupIndexCapsAction.Response());
                } else {
                    executeAsyncWithOrigin(
                        client,
                        ML_ORIGIN,
                        GetRollupIndexCapsAction.INSTANCE,
                        new GetRollupIndexCapsAction.Request(indices),
                        getRollupIndexCapsActionHandler
                    );
                }
            });
        } else {
            putDatafeed(request, threadPool.getThreadContext().getHeaders(), state, listener);
        }
    }

    public void getDatafeeds(GetDatafeedsAction.Request request, ActionListener<QueryPage<DatafeedConfig>> listener) {

        datafeedConfigProvider.expandDatafeedConfigs(
            request.getDatafeedId(),
            request.allowNoMatch(),
            ActionListener.wrap(datafeedBuilders ->

            // Build datafeeds
            listener.onResponse(
                new QueryPage<>(
                    datafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList()),
                    datafeedBuilders.size(),
                    DatafeedConfig.RESULTS_FIELD
                )
            ), listener::onFailure)
        );
    }

    public void getDatafeedsByJobIds(Set<String> jobIds, ActionListener<Map<String, DatafeedConfig.Builder>> listener) {
        datafeedConfigProvider.findDatafeedsByJobIds(jobIds, listener);
    }

    public void updateDatafeed(
        UpdateDatafeedAction.Request request,
        ClusterState state,
        SecurityContext securityContext,
        ThreadPool threadPool,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        // Check datafeed is stopped
        if (getDatafeedTask(state, request.getUpdate().getId()) != null) {
            listener.onFailure(
                ExceptionsHelper.conflictStatusException(
                    Messages.getMessage(
                        Messages.DATAFEED_CANNOT_UPDATE_IN_CURRENT_STATE,
                        request.getUpdate().getId(),
                        DatafeedState.STARTED
                    )
                )
            );
            return;
        }

        Runnable doUpdate = () -> useSecondaryAuthIfAvailable(securityContext, () -> {
            final Map<String, String> headers = threadPool.getThreadContext().getHeaders();
            datafeedConfigProvider.updateDatefeedConfig(
                request.getUpdate().getId(),
                request.getUpdate(),
                headers,
                jobConfigProvider::validateDatafeedJob,
                ActionListener.wrap(
                    updatedConfig -> listener.onResponse(new PutDatafeedAction.Response(updatedConfig)),
                    listener::onFailure
                )
            );
        });

        // Obviously if we're updating a datafeed it's impossible that the config index has no mappings at
        // all, but if we rewrite the datafeed config we may add new fields that require the latest mappings
        ElasticsearchMappings.addDocMappingIfMissing(
            MlConfigIndex.indexName(),
            MlConfigIndex::mapping,
            client,
            state,
            request.masterNodeTimeout(),
            ActionListener.wrap(bool -> doUpdate.run(), listener::onFailure)
        );
    }

    public void deleteDatafeed(DeleteDatafeedAction.Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        if (getDatafeedTask(state, request.getDatafeedId()) != null) {
            listener.onFailure(
                ExceptionsHelper.conflictStatusException(
                    Messages.getMessage(Messages.DATAFEED_CANNOT_DELETE_IN_CURRENT_STATE, request.getDatafeedId(), DatafeedState.STARTED)
                )
            );
            return;
        }

        String datafeedId = request.getDatafeedId();

        datafeedConfigProvider.getDatafeedConfig(datafeedId, ActionListener.wrap(datafeedConfigBuilder -> {
            String jobId = datafeedConfigBuilder.build().getJobId();
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, jobId);
            jobDataDeleter.deleteDatafeedTimingStats(
                ActionListener.wrap(
                    unused1 -> datafeedConfigProvider.deleteDatafeedConfig(
                        datafeedId,
                        ActionListener.wrap(unused2 -> listener.onResponse(AcknowledgedResponse.TRUE), listener::onFailure)
                    ),
                    listener::onFailure
                )
            );
        }, listener::onFailure));

    }

    private PersistentTasksCustomMetadata.PersistentTask<?> getDatafeedTask(ClusterState state, String datafeedId) {
        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        return MlTasks.getDatafeedTask(datafeedId, tasks);
    }

    private void handlePrivsResponse(
        String username,
        PutDatafeedAction.Request request,
        HasPrivilegesResponse response,
        ClusterState clusterState,
        ThreadPool threadPool,
        ActionListener<PutDatafeedAction.Response> listener
    ) throws IOException {
        if (response.isCompleteMatch()) {
            putDatafeed(request, threadPool.getThreadContext().getHeaders(), clusterState, listener);
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            for (ResourcePrivileges index : response.getIndexPrivileges()) {
                builder.field(index.getResource());
                builder.map(index.getPrivileges());
            }
            builder.endObject();

            listener.onFailure(
                Exceptions.authorizationError(
                    "Cannot create datafeed [{}]" + " because user {} lacks permissions on the indices: {}",
                    request.getDatafeed().getId(),
                    username,
                    Strings.toString(builder)
                )
            );
        }
    }

    private void putDatafeed(
        PutDatafeedAction.Request request,
        Map<String, String> headers,
        ClusterState clusterState,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        DatafeedConfig.validateAggregations(request.getDatafeed().getParsedAggregations(xContentRegistry));

        CheckedConsumer<Boolean, Exception> mappingsUpdated = ok -> datafeedConfigProvider.putDatafeedConfig(
            request.getDatafeed(),
            headers,
            ActionListener.wrap(
                indexResponse -> listener.onResponse(new PutDatafeedAction.Response(request.getDatafeed())),
                listener::onFailure
            )
        );

        CheckedConsumer<Boolean, Exception> validationOk = ok -> {
            if (clusterState == null) {
                logger.warn("Cannot update doc mapping because clusterState == null");
                mappingsUpdated.accept(false);
                return;
            }
            ElasticsearchMappings.addDocMappingIfMissing(
                MlConfigIndex.indexName(),
                MlConfigIndex::mapping,
                client,
                clusterState,
                request.masterNodeTimeout(),
                ActionListener.wrap(mappingsUpdated, listener::onFailure)
            );
        };

        CheckedConsumer<Boolean, Exception> jobOk = ok -> jobConfigProvider.validateDatafeedJob(
            request.getDatafeed(),
            ActionListener.wrap(validationOk, listener::onFailure)
        );

        checkJobDoesNotHaveADatafeed(request.getDatafeed().getJobId(), ActionListener.wrap(jobOk, listener::onFailure));
    }

    private void checkJobDoesNotHaveADatafeed(String jobId, ActionListener<Boolean> listener) {
        datafeedConfigProvider.findDatafeedIdsForJobIds(Collections.singletonList(jobId), ActionListener.wrap(datafeedIds -> {
            if (datafeedIds.isEmpty()) {
                listener.onResponse(Boolean.TRUE);
            } else {
                listener.onFailure(
                    ExceptionsHelper.conflictStatusException(
                        "A datafeed [" + datafeedIds.iterator().next() + "] already exists for job [" + jobId + "]"
                    )
                );
            }
        }, listener::onFailure));
    }
}
