/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.RemoteClusterResolver;
import org.elasticsearch.xpack.sql.SqlInfoTransportAction;
import org.elasticsearch.xpack.sql.SqlUsageTransportAction;
import org.elasticsearch.xpack.sql.action.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlTranslateAction;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.type.SqlDataTypeRegistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class SqlPlugin extends Plugin implements ActionPlugin {

    private final LicensedFeature.Momentary JDBC_FEATURE = LicensedFeature.momentary("sql", "jdbc", License.OperationMode.PLATINUM);
    private final LicensedFeature.Momentary ODBC_FEATURE = LicensedFeature.momentary("sql", "odbc", License.OperationMode.PLATINUM);

    private final SqlLicenseChecker sqlLicenseChecker = new SqlLicenseChecker((mode) -> {
        XPackLicenseState licenseState = getLicenseState();
        switch (mode) {
            case JDBC:
                if (JDBC_FEATURE.check(licenseState) == false) {
                    throw LicenseUtils.newComplianceException("jdbc");
                }
                break;
            case ODBC:
                if (ODBC_FEATURE.check(licenseState) == false) {
                    throw LicenseUtils.newComplianceException("odbc");
                }
                break;
            case PLAIN:
            case CLI:
                break;
            default:
                throw new IllegalArgumentException("Unknown SQL mode " + mode);
        }
    });

    public SqlPlugin(Settings settings) {}

    // overridable by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService
    ) {

        return createComponents(client, environment.settings(), clusterService, namedWriteableRegistry);
    }

    /**
     * Create components used by the sql plugin.
     */
    Collection<Object> createComponents(
        Client client,
        Settings settings,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        RemoteClusterResolver remoteClusterResolver = new RemoteClusterResolver(settings, clusterService.getClusterSettings());
        IndexResolver indexResolver = new IndexResolver(
            client,
            clusterService.getClusterName().value(),
            SqlDataTypeRegistry.INSTANCE,
            remoteClusterResolver::remoteClusters
        );
        return Arrays.asList(sqlLicenseChecker, indexResolver, new PlanExecutor(client, indexResolver, namedWriteableRegistry));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {

        return Arrays.asList(
            new RestSqlQueryAction(),
            new RestSqlTranslateAction(),
            new RestSqlClearCursorAction(),
            new RestSqlStatsAction(),
            new RestSqlAsyncGetResultsAction(),
            new RestSqlAsyncGetStatusAction(),
            new RestSqlAsyncDeleteResultsAction()
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.SQL, SqlUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.SQL, SqlInfoTransportAction.class);

        return Arrays.asList(
            new ActionHandler<>(SqlQueryAction.INSTANCE, TransportSqlQueryAction.class),
            new ActionHandler<>(SqlTranslateAction.INSTANCE, TransportSqlTranslateAction.class),
            new ActionHandler<>(SqlClearCursorAction.INSTANCE, TransportSqlClearCursorAction.class),
            new ActionHandler<>(SqlStatsAction.INSTANCE, TransportSqlStatsAction.class),
            new ActionHandler<>(SqlAsyncGetResultsAction.INSTANCE, TransportSqlAsyncGetResultsAction.class),
            new ActionHandler<>(SqlAsyncGetStatusAction.INSTANCE, TransportSqlAsyncGetStatusAction.class),
            usageAction,
            infoAction
        );
    }
}
