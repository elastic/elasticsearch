/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.transport.LinkedProjectConfigService;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Setting.Property.Dynamic;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;

public class SqlPlugin extends Plugin implements ActionPlugin {

    public static final int DEFAULT_MAX_QUERY_LENGTH = 1_000_000;

    /**
     * Maximum number of characters in an SQL query. Antlr may parse the entire
     * query into tokens to make the choices, buffering the world. There's a lot we
     * can do in the grammar to prevent that, but let's be paranoid and assume we'll
     * fail at preventing antlr from slurping in the world. Instead, let's make sure
     * that the world just isn't that big.
     */
    public static final Setting<Integer> MAX_QUERY_LENGTH_SETTING = Setting.intSetting(
        "xpack.sql.max_query_length",
        DEFAULT_MAX_QUERY_LENGTH,
        1,
        Dynamic,
        NodeScope
    );

    private final LicensedFeature.Momentary JDBC_FEATURE = LicensedFeature.momentary("sql", "jdbc", License.OperationMode.PLATINUM);
    private final LicensedFeature.Momentary ODBC_FEATURE = LicensedFeature.momentary("sql", "odbc", License.OperationMode.PLATINUM);

    @SuppressWarnings("this-escape")
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

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MAX_QUERY_LENGTH_SETTING);
    }

    // overridable by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        return createComponents(
            services.client(),
            services.environment().settings(),
            services.clusterService().getClusterName().value(),
            services.linkedProjectConfigService(),
            services.namedWriteableRegistry()
        );
    }

    /**
     * Create components used by the sql plugin.
     */
    Collection<Object> createComponents(
        Client client,
        Settings settings,
        String clusterName,
        LinkedProjectConfigService linkedProjectConfigService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        RemoteClusterResolver remoteClusterResolver = new RemoteClusterResolver(settings, linkedProjectConfigService);
        IndexResolver indexResolver = new IndexResolver(
            client,
            clusterName,
            SqlDataTypeRegistry.INSTANCE,
            remoteClusterResolver::remoteClusters
        );
        return Arrays.asList(sqlLicenseChecker, indexResolver, new PlanExecutor(client, indexResolver, namedWriteableRegistry));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {

        return Arrays.asList(
            new RestSqlQueryAction(restHandlersServices.crossProjectModeDecider()),
            new RestSqlTranslateAction(restHandlersServices.crossProjectModeDecider()),
            new RestSqlClearCursorAction(),
            new RestSqlStatsAction(),
            new RestSqlAsyncGetResultsAction(),
            new RestSqlAsyncGetStatusAction(),
            new RestSqlAsyncDeleteResultsAction()
        );
    }

    @Override
    public List<ActionHandler> getActions() {
        var usageAction = new ActionHandler(XPackUsageFeatureAction.SQL, SqlUsageTransportAction.class);
        var infoAction = new ActionHandler(XPackInfoFeatureAction.SQL, SqlInfoTransportAction.class);

        return Arrays.asList(
            new ActionHandler(SqlQueryAction.INSTANCE, TransportSqlQueryAction.class),
            new ActionHandler(SqlTranslateAction.INSTANCE, TransportSqlTranslateAction.class),
            new ActionHandler(SqlClearCursorAction.INSTANCE, TransportSqlClearCursorAction.class),
            new ActionHandler(SqlStatsAction.INSTANCE, TransportSqlStatsAction.class),
            new ActionHandler(SqlAsyncGetResultsAction.INSTANCE, TransportSqlAsyncGetResultsAction.class),
            new ActionHandler(SqlAsyncGetStatusAction.INSTANCE, TransportSqlAsyncGetStatusAction.class),
            usageAction,
            infoAction
        );
    }
}
