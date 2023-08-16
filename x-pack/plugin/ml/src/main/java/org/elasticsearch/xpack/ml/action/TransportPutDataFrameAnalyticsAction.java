/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
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
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.function.Predicate.not;
import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

public class TransportPutDataFrameAnalyticsAction extends TransportMasterNodeAction<
    PutDataFrameAnalyticsAction.Request,
    PutDataFrameAnalyticsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutDataFrameAnalyticsAction.class);

    private final XPackLicenseState licenseState;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final SecurityContext securityContext;
    private final Client client;
    private final DataFrameAnalyticsAuditor auditor;
    private final SourceDestValidator sourceDestValidator;
    private final Supplier<ByteSizeValue> maxModelMemoryLimitSupplier;

    @Inject
    public TransportPutDataFrameAnalyticsAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        Client client,
        ThreadPool threadPool,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DataFrameAnalyticsConfigProvider configProvider,
        DataFrameAnalyticsAuditor auditor
    ) {
        super(
            PutDataFrameAnalyticsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutDataFrameAnalyticsAction.Request::new,
            indexNameExpressionResolver,
            PutDataFrameAnalyticsAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.licenseState = licenseState;
        this.configProvider = configProvider;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.client = client;
        this.auditor = Objects.requireNonNull(auditor);
        this.maxModelMemoryLimitSupplier = () -> NativeMemoryCalculator.getMaxModelMemoryLimit(clusterService);

        this.sourceDestValidator = new SourceDestValidator(
            indexNameExpressionResolver,
            transportService.getRemoteClusterService(),
            null,
            null,
            clusterService.getNodeName(),
            License.OperationMode.PLATINUM.description()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataFrameAnalyticsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutDataFrameAnalyticsAction.Request request,
        ClusterState state,
        ActionListener<PutDataFrameAnalyticsAction.Response> listener
    ) {

        final DataFrameAnalyticsConfig config = request.getConfig();

        ActionListener<Boolean> sourceDestValidationListener = ActionListener.wrap(
            aBoolean -> putValidatedConfig(config, request.masterNodeTimeout(), listener),
            listener::onFailure
        );

        sourceDestValidator.validate(
            clusterService.state(),
            config.getSource().getIndex(),
            config.getDest().getIndex(),
            null,
            SourceDestValidations.ALL_VALIDATIONS,
            sourceDestValidationListener
        );
    }

    private void putValidatedConfig(
        DataFrameAnalyticsConfig config,
        TimeValue masterNodeTimeout,
        ActionListener<PutDataFrameAnalyticsAction.Response> listener
    ) {
        DataFrameAnalyticsConfig preparedForPutConfig = new DataFrameAnalyticsConfig.Builder(config, maxModelMemoryLimitSupplier.get())
            .setCreateTime(Instant.now())
            .setVersion(MlConfigVersion.CURRENT)
            .build();

        if (securityContext != null) {
            useSecondaryAuthIfAvailable(securityContext, () -> {
                final String username = securityContext.getUser().principal();
                // DFA doesn't support CCS, but if it did it would need this filter, so it's safest to have the filter
                // in place even though it's a no-op.
                // TODO: Remove this filter once https://github.com/elastic/elasticsearch/issues/67798 is fixed.
                final String[] sourceIndices = Arrays.stream(preparedForPutConfig.getSource().getIndex())
                    .filter(not(RemoteClusterLicenseChecker::isRemoteIndex))
                    .toArray(String[]::new);
                RoleDescriptor.IndicesPrivileges destIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                    .indices(preparedForPutConfig.getDest().getIndex())
                    .privileges("read", "index", "create_index")
                    .build();

                HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
                privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
                privRequest.username(username);
                privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
                RoleDescriptor.IndicesPrivileges[] indicesPrivileges;
                if (sourceIndices.length > 0) {
                    RoleDescriptor.IndicesPrivileges sourceIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                        .indices(sourceIndices)
                        .privileges("read")
                        .build();
                    indicesPrivileges = new RoleDescriptor.IndicesPrivileges[] { sourceIndexPrivileges, destIndexPrivileges };
                } else {
                    indicesPrivileges = new RoleDescriptor.IndicesPrivileges[] { destIndexPrivileges };
                }
                privRequest.indexPrivileges(indicesPrivileges);

                ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                    r -> handlePrivsResponse(username, preparedForPutConfig, r, masterNodeTimeout, listener),
                    listener::onFailure
                );

                client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
            });
        } else {
            updateDocMappingAndPutConfig(
                preparedForPutConfig,
                threadPool.getThreadContext().getHeaders(),
                masterNodeTimeout,
                ActionListener.wrap(
                    finalConfig -> listener.onResponse(new PutDataFrameAnalyticsAction.Response(finalConfig)),
                    listener::onFailure
                )
            );
        }
    }

    private void handlePrivsResponse(
        String username,
        DataFrameAnalyticsConfig memoryCappedConfig,
        HasPrivilegesResponse response,
        TimeValue masterNodeTimeout,
        ActionListener<PutDataFrameAnalyticsAction.Response> listener
    ) throws IOException {
        if (response.isCompleteMatch()) {
            updateDocMappingAndPutConfig(
                memoryCappedConfig,
                threadPool.getThreadContext().getHeaders(),
                masterNodeTimeout,
                ActionListener.wrap(
                    finalConfig -> listener.onResponse(new PutDataFrameAnalyticsAction.Response(finalConfig)),
                    listener::onFailure
                )
            );
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
                    "Cannot create data frame analytics [{}]" + " because user {} lacks permissions on the indices: {}",
                    memoryCappedConfig.getId(),
                    username,
                    Strings.toString(builder)
                )
            );
        }
    }

    private void updateDocMappingAndPutConfig(
        DataFrameAnalyticsConfig config,
        Map<String, String> headers,
        TimeValue masterNodeTimeout,
        ActionListener<DataFrameAnalyticsConfig> listener
    ) {
        ActionListener<DataFrameAnalyticsConfig> auditingListener = ActionListener.wrap(finalConfig -> {
            auditor.info(
                finalConfig.getId(),
                Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_CREATED, finalConfig.getAnalysis().getWriteableName())
            );
            listener.onResponse(finalConfig);
        }, listener::onFailure);

        ClusterState clusterState = clusterService.state();
        if (clusterState == null) {
            logger.warn("Cannot update doc mapping because clusterState == null");
            configProvider.put(config, headers, masterNodeTimeout, auditingListener);
            return;
        }
        ElasticsearchMappings.addDocMappingIfMissing(
            MlConfigIndex.indexName(),
            MlConfigIndex::mapping,
            client,
            clusterState,
            masterNodeTimeout,
            ActionListener.wrap(unused -> configProvider.put(config, headers, masterNodeTimeout, auditingListener), listener::onFailure)
        );
    }

    @Override
    protected void doExecute(
        Task task,
        PutDataFrameAnalyticsAction.Request request,
        ActionListener<PutDataFrameAnalyticsAction.Response> listener
    ) {
        if (MachineLearningField.ML_API_FEATURE.check(licenseState)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
