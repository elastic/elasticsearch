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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;
import org.elasticsearch.xpack.sql.analysis.catalog.FilteredCatalog;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.cli.action.CliAction;
import org.elasticsearch.xpack.sql.plugin.cli.action.CliHttpHandler;
import org.elasticsearch.xpack.sql.plugin.cli.action.TransportCliAction;
import org.elasticsearch.xpack.sql.plugin.jdbc.action.JdbcAction;
import org.elasticsearch.xpack.sql.plugin.jdbc.action.JdbcHttpHandler;
import org.elasticsearch.xpack.sql.plugin.jdbc.action.TransportJdbcAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.TransportSqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.rest.RestSqlAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class SqlPlugin implements ActionPlugin {

    private final SqlLicenseChecker sqlLicenseChecker;

    public SqlPlugin(SqlLicenseChecker sqlLicenseChecker) {
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    /**
     * Create components used by the sql plugin.
     * @param catalogFilter if non-null it is a filter for the catalog to apply security
     */
    public Collection<Object> createComponents(Client client, ClusterService clusterService,
            @Nullable FilteredCatalog.Filter catalogFilter) {
        EsCatalog esCatalog = new EsCatalog(() -> clusterService.state());
        Catalog catalog = catalogFilter == null ? esCatalog : new FilteredCatalog(esCatalog, catalogFilter);
        return Arrays.asList(
                esCatalog,  // Added as a component so that it can get IndexNameExpressionResolver injected.
                sqlLicenseChecker,
                new PlanExecutor(client, catalog));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, 
            ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, 
            IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {

        return Arrays.asList(new RestSqlAction(settings, restController),
                             new CliHttpHandler(settings, restController),
                             new JdbcHttpHandler(settings, restController));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(SqlAction.INSTANCE, TransportSqlAction.class),
                             new ActionHandler<>(CliAction.INSTANCE, TransportCliAction.class),
                             new ActionHandler<>(JdbcAction.INSTANCE, TransportJdbcAction.class));
    }
}
