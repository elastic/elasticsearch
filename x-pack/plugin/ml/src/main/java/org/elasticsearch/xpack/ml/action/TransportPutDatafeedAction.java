/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

public class TransportPutDatafeedAction extends TransportMasterNodeAction<PutDatafeedAction.Request, PutDatafeedAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutDatafeedAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final SecurityContext securityContext;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobConfigProvider jobConfigProvider;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportPutDatafeedAction(Settings settings, TransportService transportService,
                                      ClusterService clusterService, ThreadPool threadPool, Client client,
                                      XPackLicenseState licenseState, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      NamedXContentRegistry xContentRegistry) {
        super(PutDatafeedAction.NAME, transportService, clusterService, threadPool,
                actionFilters, PutDatafeedAction.Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.client = client;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
                new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry);
        this.jobConfigProvider = new JobConfigProvider(client, xContentRegistry);
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDatafeedAction.Response read(StreamInput in) throws IOException {
        return new PutDatafeedAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, PutDatafeedAction.Request request, ClusterState state,
                                   ActionListener<PutDatafeedAction.Response> listener) {
        // If security is enabled only create the datafeed if the user requesting creation has
        // permission to read the indices the datafeed is going to read from
        if (licenseState.isSecurityEnabled()) {
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
                    r -> handlePrivsResponse(username, request, r, state, listener),
                    listener::onFailure);

                ActionListener<GetRollupIndexCapsAction.Response> getRollupIndexCapsActionHandler = ActionListener.wrap(
                    response -> {
                        if (response.getJobs().isEmpty()) { // This means no rollup indexes are in the config
                            indicesPrivilegesBuilder.privileges(SearchAction.NAME);
                        } else {
                            indicesPrivilegesBuilder.privileges(SearchAction.NAME, RollupSearchAction.NAME);
                        }
                        privRequest.indexPrivileges(indicesPrivilegesBuilder.build());
                        client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
                    },
                    e -> {
                        if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                            indicesPrivilegesBuilder.privileges(SearchAction.NAME);
                            privRequest.indexPrivileges(indicesPrivilegesBuilder.build());
                            client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                );
                if (RemoteClusterLicenseChecker.containsRemoteIndex(request.getDatafeed().getIndices())) {
                    getRollupIndexCapsActionHandler.onResponse(new GetRollupIndexCapsAction.Response());
                } else {
                    executeAsyncWithOrigin(client,
                        ML_ORIGIN,
                        GetRollupIndexCapsAction.INSTANCE,
                        new GetRollupIndexCapsAction.Request(indices),
                        getRollupIndexCapsActionHandler);
                }
            });
        } else {
            putDatafeed(request, threadPool.getThreadContext().getHeaders(), state, listener);
        }
    }

    private void handlePrivsResponse(String username,
                                     PutDatafeedAction.Request request,
                                     HasPrivilegesResponse response,
                                     ClusterState clusterState,
                                     ActionListener<PutDatafeedAction.Response> listener) throws IOException {
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

            listener.onFailure(Exceptions.authorizationError("Cannot create datafeed [{}]" +
                            " because user {} lacks permissions on the indices: {}",
                    request.getDatafeed().getId(), username, Strings.toString(builder)));
        }
    }

    private void putDatafeed(PutDatafeedAction.Request request,
                             Map<String, String> headers,
                             ClusterState clusterState,
                             ActionListener<PutDatafeedAction.Response> listener) {

        String datafeedId = request.getDatafeed().getId();
        String jobId = request.getDatafeed().getJobId();
        ElasticsearchException validationError = checkConfigsAreNotDefinedInClusterState(datafeedId, jobId);
        if (validationError != null) {
            listener.onFailure(validationError);
            return;
        }
        DatafeedConfig.validateAggregations(request.getDatafeed().getParsedAggregations(xContentRegistry));

        CheckedConsumer<Boolean, Exception> mappingsUpdated = ok -> {
            datafeedConfigProvider.putDatafeedConfig(
                request.getDatafeed(),
                headers,
                ActionListener.wrap(
                    indexResponse -> listener.onResponse(new PutDatafeedAction.Response(request.getDatafeed())),
                    listener::onFailure
            ));
        };

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
                ActionListener.wrap(mappingsUpdated, listener::onFailure));
        };

        CheckedConsumer<Boolean, Exception> jobOk = ok ->
            jobConfigProvider.validateDatafeedJob(request.getDatafeed(), ActionListener.wrap(validationOk, listener::onFailure));

        checkJobDoesNotHaveADatafeed(jobId, ActionListener.wrap(jobOk, listener::onFailure));
    }

    /**
     * Returns an exception if a datafeed with the same Id is defined in the
     * cluster state or the job is in the cluster state and already has a datafeed
     */
    @Nullable
    private ElasticsearchException checkConfigsAreNotDefinedInClusterState(String datafeedId, String jobId) {
        ClusterState clusterState = clusterService.state();
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);

        if (mlMetadata.getDatafeed(datafeedId) != null) {
            return ExceptionsHelper.datafeedAlreadyExists(datafeedId);
        }

        if (mlMetadata.getDatafeedByJobId(jobId).isPresent()) {
            return ExceptionsHelper.conflictStatusException("Cannot create datafeed [" + datafeedId + "] as a " +
                    "job [" + jobId + "] defined in the cluster state references a datafeed with the same Id");
        }

        return null;
    }

    private void checkJobDoesNotHaveADatafeed(String jobId, ActionListener<Boolean> listener) {
        datafeedConfigProvider.findDatafeedsForJobIds(Collections.singletonList(jobId), ActionListener.wrap(
                datafeedIds -> {
                    if (datafeedIds.isEmpty()) {
                        listener.onResponse(Boolean.TRUE);
                    } else {
                        listener.onFailure(ExceptionsHelper.conflictStatusException("A datafeed [" + datafeedIds.iterator().next()
                                + "] already exists for job [" + jobId + "]"));
                    }
                },
                listener::onFailure
        ));
    }

    @Override
    protected ClusterBlockException checkBlock(PutDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void doExecute(Task task, PutDatafeedAction.Request request, ActionListener<PutDatafeedAction.Response> listener) {
        if (licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
