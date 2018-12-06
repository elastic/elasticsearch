/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.sql.SqlFeatureSet;
import org.elasticsearch.xpack.sql.action.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlTranslateAction;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public class SqlPlugin extends Plugin implements ActionPlugin {

    private final boolean enabled;
    private final SqlLicenseChecker sqlLicenseChecker;

    SqlPlugin(boolean enabled, SqlLicenseChecker sqlLicenseChecker) {
        this.enabled = enabled;
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    public SqlPlugin(Settings settings) {
        this(XPackSettings.SQL_ENABLED.get(settings), new SqlLicenseChecker(
                (mode) -> {
                    XPackLicenseState licenseState = XPackPlugin.getSharedLicenseState();
                    switch (mode) {
                        case JDBC:
                            if (licenseState.isJdbcAllowed() == false) {
                                throw LicenseUtils.newComplianceException("jdbc");
                            }
                            break;
                        case ODBC:
                            if (licenseState.isOdbcAllowed() == false) {
                                throw LicenseUtils.newComplianceException("odbc");
                            }
                            break;
                        case PLAIN:
                            if (licenseState.isSqlAllowed() == false) {
                                throw LicenseUtils.newComplianceException(XPackField.SQL);
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown SQL mode " + mode);
                    }
                }
        ));
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {

        return createComponents(client, clusterService.getClusterName().value(), namedWriteableRegistry);
    }

    /**
     * Create components used by the sql plugin.
     */
    Collection<Object> createComponents(Client client, String clusterName, NamedWriteableRegistry namedWriteableRegistry) {
        if (false == enabled) {
            return emptyList();
        }
        IndexResolver indexResolver = new IndexResolver(client, clusterName);
        return Arrays.asList(sqlLicenseChecker, indexResolver, new PlanExecutor(client, indexResolver, namedWriteableRegistry));
    }

    @Override
    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(b -> XPackPlugin.bindFeatureSet(b, SqlFeatureSet.class));
        return modules;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                             ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {

        if (false == enabled) {
            return emptyList();
        }

        return Arrays.asList(new RestSqlQueryAction(settings, restController),
                new RestSqlTranslateAction(settings, restController),
                new RestSqlClearCursorAction(settings, restController),
                new RestSqlStatsAction(settings, restController));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (false == enabled) {
            return emptyList();
        }

        return Arrays.asList(new ActionHandler<>(SqlQueryAction.INSTANCE, TransportSqlQueryAction.class),
                new ActionHandler<>(SqlTranslateAction.INSTANCE, TransportSqlTranslateAction.class),
                new ActionHandler<>(SqlClearCursorAction.INSTANCE, TransportSqlClearCursorAction.class),
                new ActionHandler<>(SqlStatsAction.INSTANCE, TransportSqlStatsAction.class));
    }
}
