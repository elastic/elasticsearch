/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.ml.EsqlDatafeedQueryParser;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.view.ViewService;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The real ES|QL datafeed parser over a cluster with default settings, for tests that parse ES|QL datafeed queries. */
public final class EsqlParserTestSupport {

    private EsqlParserTestSupport() {}

    public static EsqlQueryParser parser() {
        return parser(Settings.EMPTY);
    }

    /**
     * The parser reads only the settings, the registered cluster settings and an empty cluster state, so a mocked {@link ClusterService}
     * stands in for one: a real service needs a thread pool and a lifecycle that a unit test of a parsing rule should not own.
     */
    public static EsqlQueryParser parser(Settings nodeSettings) {
        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);
        registered.addAll(QuerySettings.clusterSettings());
        registered.addAll(InferenceSettings.getSettings());
        registered.add(ViewService.MAX_VIEWS_COUNT_SETTING);
        registered.add(ViewService.MAX_VIEW_LENGTH_SETTING);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(nodeSettings, registered));
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(mock(ThreadPool.class));
        EsqlParser esqlParser = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry()));
        EsqlDatafeedQueryParser.Services services = new EsqlDatafeedQueryParser.Services(
            esqlParser,
            clusterService,
            DefaultProjectResolver.INSTANCE,
            new ViewService(clusterService, esqlParser, (statement, viewName, listener) -> listener.onResponse(null)),
            new CrossProjectModeDecider(nodeSettings),
            new InferenceService(client, clusterService),
            new XPackLicenseState(System::currentTimeMillis)
        );
        return EsqlDatafeedQueryParser.withServices(() -> services);
    }
}
