/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.ml.dataframe.SourceDestValidations;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public class TransportPutDataFrameAnalyticsAction
    extends TransportMasterNodeAction<PutDataFrameAnalyticsAction.Request, PutDataFrameAnalyticsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutDataFrameAnalyticsAction.class);

    private final XPackLicenseState licenseState;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final SecurityContext securityContext;
    private final Client client;
    private final DataFrameAnalyticsAuditor auditor;
    private final SourceDestValidator sourceDestValidator;

    private volatile ByteSizeValue maxModelMemoryLimit;

    @Inject
    public TransportPutDataFrameAnalyticsAction(Settings settings, TransportService transportService, ActionFilters actionFilters,
                                                XPackLicenseState licenseState, Client client, ThreadPool threadPool,
                                                ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                                                DataFrameAnalyticsConfigProvider configProvider, DataFrameAnalyticsAuditor auditor) {
        super(PutDataFrameAnalyticsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PutDataFrameAnalyticsAction.Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.configProvider = configProvider;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.client = client;
        this.auditor = Objects.requireNonNull(auditor);

        maxModelMemoryLimit = MachineLearningField.MAX_MODEL_MEMORY_LIMIT.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MachineLearningField.MAX_MODEL_MEMORY_LIMIT, this::setMaxModelMemoryLimit);

        this.sourceDestValidator = new SourceDestValidator(
            indexNameExpressionResolver,
            transportService.getRemoteClusterService(),
            null,
            clusterService.getNodeName(),
            License.OperationMode.PLATINUM.description()
        );
    }

    private void setMaxModelMemoryLimit(ByteSizeValue maxModelMemoryLimit) {
        this.maxModelMemoryLimit = maxModelMemoryLimit;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDataFrameAnalyticsAction.Response read(StreamInput in) throws IOException {
        return new PutDataFrameAnalyticsAction.Response(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataFrameAnalyticsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, PutDataFrameAnalyticsAction.Request request, ClusterState state,
                                   ActionListener<PutDataFrameAnalyticsAction.Response> listener) {

        final DataFrameAnalyticsConfig config = request.getConfig();

        ActionListener<Boolean> sourceDestValidationListener = ActionListener.wrap(
            aBoolean -> putValidatedConfig(config, listener),
            listener::onFailure
        );

        sourceDestValidator.validate(clusterService.state(), config.getSource().getIndex(), config.getDest().getIndex(),
            SourceDestValidations.ALL_VALIDATIONS, sourceDestValidationListener);
    }

    private void putValidatedConfig(DataFrameAnalyticsConfig config, ActionListener<PutDataFrameAnalyticsAction.Response> listener) {
        DataFrameAnalyticsConfig preparedForPutConfig =
            new DataFrameAnalyticsConfig.Builder(config, maxModelMemoryLimit)
                .setCreateTime(Instant.now())
                .setVersion(Version.CURRENT)
                .build();

        if (licenseState.isAuthAllowed()) {
            final String username = securityContext.getUser().principal();
            RoleDescriptor.IndicesPrivileges sourceIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(preparedForPutConfig.getSource().getIndex())
                .privileges("read")
                .build();
            RoleDescriptor.IndicesPrivileges destIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(preparedForPutConfig.getDest().getIndex())
                .privileges("read", "index", "create_index")
                .build();

            HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
            privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
            privRequest.username(username);
            privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
            privRequest.indexPrivileges(sourceIndexPrivileges, destIndexPrivileges);

            ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                r -> handlePrivsResponse(username, preparedForPutConfig, r, listener),
                listener::onFailure);

            client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
        } else {
            updateDocMappingAndPutConfig(
                preparedForPutConfig,
                threadPool.getThreadContext().getHeaders(),
                ActionListener.wrap(
                    indexResponse -> listener.onResponse(new PutDataFrameAnalyticsAction.Response(preparedForPutConfig)),
                    listener::onFailure
                ));
        }
    }

    private void handlePrivsResponse(String username, DataFrameAnalyticsConfig memoryCappedConfig,
                                     HasPrivilegesResponse response,
                                     ActionListener<PutDataFrameAnalyticsAction.Response> listener) throws IOException {
        if (response.isCompleteMatch()) {
            updateDocMappingAndPutConfig(
                memoryCappedConfig,
                threadPool.getThreadContext().getHeaders(),
                ActionListener.wrap(
                    indexResponse -> listener.onResponse(new PutDataFrameAnalyticsAction.Response(memoryCappedConfig)),
                    listener::onFailure
            ));
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            for (ResourcePrivileges index : response.getIndexPrivileges()) {
                builder.field(index.getResource());
                builder.map(index.getPrivileges());
            }
            builder.endObject();

            listener.onFailure(Exceptions.authorizationError("Cannot create data frame analytics [{}]" +
                    " because user {} lacks permissions on the indices: {}",
                    memoryCappedConfig.getId(), username, Strings.toString(builder)));
        }
    }

    private void updateDocMappingAndPutConfig(DataFrameAnalyticsConfig config,
                                              Map<String, String> headers,
                                              ActionListener<IndexResponse> listener) {
        ClusterState clusterState = clusterService.state();
        if (clusterState == null) {
            logger.warn("Cannot update doc mapping because clusterState == null");
            configProvider.put(config, headers, listener);
            return;
        }
        ElasticsearchMappings.addDocMappingIfMissing(
            AnomalyDetectorsIndex.configIndexName(),
            ElasticsearchMappings::configMapping,
            client,
            clusterState,
            ActionListener.wrap(
                unused -> configProvider.put(config, headers, ActionListener.wrap(
                    indexResponse -> {
                        auditor.info(
                            config.getId(),
                            Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_CREATED, config.getAnalysis().getWriteableName()));
                        listener.onResponse(indexResponse);
                    },
                    listener::onFailure)),
                listener::onFailure));
    }

    @Override
    protected void doExecute(Task task, PutDataFrameAnalyticsAction.Request request,
                             ActionListener<PutDataFrameAnalyticsAction.Response> listener) {
        if (licenseState.isMachineLearningAllowed()) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
