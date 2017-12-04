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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.TransportSqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.rest.RestSqlAction;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public class SqlPlugin implements ActionPlugin {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Cursor.getNamedWriteables();
    }

    private final boolean enabled;
    private final SqlLicenseChecker sqlLicenseChecker;
    private IndexResolver indexResolver;

    public SqlPlugin(boolean enabled, SqlLicenseChecker sqlLicenseChecker) {
        this.enabled = enabled;
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    /**
     * Create components used by the sql plugin.
     */
    public Collection<Object> createComponents(Client client) {
        if (false == enabled) {
            return emptyList();
        }
        indexResolver = new IndexResolver(client);
        return Arrays.asList(sqlLicenseChecker, indexResolver, new PlanExecutor(client, indexResolver));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
            ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {

        if (false == enabled) {
            return emptyList();
        }

        return Arrays.asList(new RestSqlAction(settings, restController),
                             new SqlTranslateAction.RestAction(settings, restController),
                             new RestSqlCliAction(settings, restController),
                             new RestSqlJdbcAction(settings, restController, sqlLicenseChecker, indexResolver));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (false == enabled) {
            return emptyList();
        }

        return Arrays.asList(new ActionHandler<>(SqlAction.INSTANCE, TransportSqlAction.class),
                             new ActionHandler<>(SqlTranslateAction.INSTANCE, SqlTranslateAction.TransportAction.class));
    }
}
