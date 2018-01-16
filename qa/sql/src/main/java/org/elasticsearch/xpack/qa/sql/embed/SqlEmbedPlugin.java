/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.embed;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.RestSqlClearCursorAction;
import org.elasticsearch.xpack.sql.plugin.RestSqlListColumnsAction;
import org.elasticsearch.xpack.sql.plugin.RestSqlListTablesAction;
import org.elasticsearch.xpack.sql.plugin.RestSqlQueryAction;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.plugin.SqlLicenseChecker;
import org.elasticsearch.xpack.sql.plugin.SqlListColumnsAction;
import org.elasticsearch.xpack.sql.plugin.SqlListTablesAction;
import org.elasticsearch.xpack.sql.plugin.SqlQueryAction;
import org.elasticsearch.xpack.sql.plugin.SqlTranslateAction;
import org.elasticsearch.xpack.sql.plugin.TransportSqlClearCursorAction;
import org.elasticsearch.xpack.sql.plugin.TransportSqlListColumnsAction;
import org.elasticsearch.xpack.sql.plugin.TransportSqlListTablesAction;
import org.elasticsearch.xpack.sql.plugin.TransportSqlQueryAction;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin for adding SQL functionality to internal test cluster
 * <p>
 * It is used by in the embeded test mode by {@link EmbeddedJdbcServer}.
 */
public class SqlEmbedPlugin extends Plugin implements ActionPlugin {

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Cursor.getNamedWriteables();
    }

    private final SqlLicenseChecker sqlLicenseChecker = new SqlLicenseChecker(mode -> { });

    public SqlEmbedPlugin() {
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        IndexResolver indexResolver = new IndexResolver(client);
        return Arrays.asList(sqlLicenseChecker, indexResolver, new PlanExecutor(client, indexResolver));
    }


    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                             ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {

        return Arrays.asList(new RestSqlQueryAction(settings, restController),
                new SqlTranslateAction.RestAction(settings, restController),
                new RestSqlClearCursorAction(settings, restController),
                new RestSqlListTablesAction(settings, restController),
                new RestSqlListColumnsAction(settings, restController));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(SqlQueryAction.INSTANCE, TransportSqlQueryAction.class),
                new ActionHandler<>(SqlTranslateAction.INSTANCE, SqlTranslateAction.TransportAction.class),
                new ActionHandler<>(SqlClearCursorAction.INSTANCE, TransportSqlClearCursorAction.class),
                new ActionHandler<>(SqlListTablesAction.INSTANCE, TransportSqlListTablesAction.class),
                new ActionHandler<>(SqlListColumnsAction.INSTANCE, TransportSqlListColumnsAction.class));
    }
}
