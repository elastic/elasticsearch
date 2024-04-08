/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType;
import org.elasticsearch.xpack.application.rules.QueryRulesIndexService;
import org.elasticsearch.xpack.application.rules.QueryRulesetListItem;
import org.elasticsearch.xpack.application.rules.action.ListQueryRulesetsAction;
import org.elasticsearch.xpack.application.search.action.ListSearchApplicationAction;
import org.elasticsearch.xpack.application.utils.LicenseUtils;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.MAX_RULE_COUNT;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.MIN_RULE_COUNT;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.RULE_CRITERIA_TOTAL_COUNTS;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.TOTAL_COUNT;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.TOTAL_RULE_COUNT;

public class EnterpriseSearchUsageTransportAction extends XPackUsageFeatureTransportAction {
    private static final Logger logger = LogManager.getLogger(EnterpriseSearchUsageTransportAction.class);
    private final XPackLicenseState licenseState;
    private final OriginSettingClient clientWithOrigin;
    private final IndicesAdminClient indicesAdminClient;

    private final boolean enabled;

    @Inject
    public EnterpriseSearchUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        XPackLicenseState licenseState,
        Client client
    ) {
        super(
            XPackUsageFeatureAction.ENTERPRISE_SEARCH.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.licenseState = licenseState;
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.indicesAdminClient = clientWithOrigin.admin().indices();
        this.enabled = XPackSettings.ENTERPRISE_SEARCH_ENABLED.get(settings);
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        if (enabled == false) {
            EnterpriseSearchFeatureSetUsage usage = new EnterpriseSearchFeatureSetUsage(
                LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                enabled,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            listener.onResponse(new XPackUsageFeatureResponse(usage));
            return;
        }

        Map<String, Object> searchApplicationsUsage = new HashMap<>();
        Map<String, Object> analyticsCollectionsUsage = new HashMap<>();
        Map<String, Object> queryRulesUsage = new HashMap<>();

        // Step 3: Fetch search applications count and return usage
        ListSearchApplicationAction.Request searchApplicationsCountRequest = new ListSearchApplicationAction.Request(
            "*",
            new PageParams(0, 0)
        );
        ActionListener<ListSearchApplicationAction.Response> searchApplicationsCountListener = ActionListener.wrap(response -> {
            addSearchApplicationsUsage(response, searchApplicationsUsage);
            listener.onResponse(
                new XPackUsageFeatureResponse(
                    new EnterpriseSearchFeatureSetUsage(
                        LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                        enabled,
                        searchApplicationsUsage,
                        analyticsCollectionsUsage,
                        queryRulesUsage
                    )
                )
            );
        }, e -> {
            listener.onResponse(
                new XPackUsageFeatureResponse(
                    new EnterpriseSearchFeatureSetUsage(
                        LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                        enabled,
                        Collections.emptyMap(),
                        analyticsCollectionsUsage,
                        queryRulesUsage
                    )
                )
            );
        });

        // Step 2: Fetch query rules stats

        ActionListener<ListQueryRulesetsAction.Response> listQueryRulesetsListener = ActionListener.wrap(response -> {
            addQueryRulesetUsage(response, queryRulesUsage);
            clientWithOrigin.execute(ListSearchApplicationAction.INSTANCE, searchApplicationsCountRequest, searchApplicationsCountListener);
        },
            e -> {
                clientWithOrigin.execute(
                    ListSearchApplicationAction.INSTANCE,
                    searchApplicationsCountRequest,
                    searchApplicationsCountListener
                );
            }
        );

        IndicesStatsRequest indicesStatsRequest = indicesAdminClient.prepareStats(QueryRulesIndexService.QUERY_RULES_ALIAS_NAME)
            .setDocs(true)
            .request();

        // Step 1: Fetch analytics collections count
        GetAnalyticsCollectionAction.Request analyticsCollectionsCountRequest = new GetAnalyticsCollectionAction.Request(
            new String[] { "*" }
        );

        ActionListener<GetAnalyticsCollectionAction.Response> analyticsCollectionsCountListener = ActionListener.wrap(response -> {
            addAnalyticsCollectionsUsage(response, analyticsCollectionsUsage);
            indicesAdminClient.execute(IndicesStatsAction.INSTANCE, indicesStatsRequest, new ActionListener<>() {
                @Override
                public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                    Map<String, IndexStats> indicesStats = indicesStatsResponse.getIndices();
                    int queryRulesetCount = indicesStats.values()
                        .stream()
                        .mapToInt(indexShardStats -> (int) indexShardStats.getPrimaries().getDocs().getCount())
                        .sum();

                    ListQueryRulesetsAction.Request queryRulesetsCountRequest = new ListQueryRulesetsAction.Request(
                        new PageParams(0, queryRulesetCount)
                    );
                    clientWithOrigin.execute(ListQueryRulesetsAction.INSTANCE, queryRulesetsCountRequest, listQueryRulesetsListener);
                }

                @Override
                public void onFailure(Exception e) {
                    ListQueryRulesetsAction.Request queryRulesetsCountRequest = new ListQueryRulesetsAction.Request(new PageParams(0, 0));
                    clientWithOrigin.execute(ListQueryRulesetsAction.INSTANCE, queryRulesetsCountRequest, listQueryRulesetsListener);
                }
            });
        }, e -> {
            ListQueryRulesetsAction.Request queryRulesetsCountRequest = new ListQueryRulesetsAction.Request(new PageParams(0, 0));
            clientWithOrigin.execute(ListQueryRulesetsAction.INSTANCE, queryRulesetsCountRequest, listQueryRulesetsListener);
        });

        // Step 0: Kick off requests
        clientWithOrigin.execute(
            GetAnalyticsCollectionAction.INSTANCE,
            analyticsCollectionsCountRequest,
            analyticsCollectionsCountListener
        );
    }

    private static void addSearchApplicationsUsage(
        ListSearchApplicationAction.Response response,
        Map<String, Object> searchApplicationsUsage
    ) {
        long count = response.queryPage().count();
        searchApplicationsUsage.put(EnterpriseSearchFeatureSetUsage.COUNT, count);
    }

    private static void addAnalyticsCollectionsUsage(
        GetAnalyticsCollectionAction.Response response,
        Map<String, Object> analyticsCollectionsUsage
    ) {
        long count = response.getAnalyticsCollections().size();
        analyticsCollectionsUsage.put(EnterpriseSearchFeatureSetUsage.COUNT, count);
    }

    private void addQueryRulesetUsage(ListQueryRulesetsAction.Response response, Map<String, Object> queryRulesUsage) {
        List<QueryRulesetListItem> results = response.queryPage().results();
        IntSummaryStatistics ruleStats = results.stream().mapToInt(QueryRulesetListItem::ruleTotalCount).summaryStatistics();

        Map<QueryRuleCriteriaType, Integer> criteriaTypeCountMap = new EnumMap<>(QueryRuleCriteriaType.class);
        results.stream()
            .flatMap(result -> result.criteriaTypeToCountMap().entrySet().stream())
            .forEach(entry -> criteriaTypeCountMap.merge(entry.getKey(), entry.getValue(), Integer::sum));

        Map<String, Object> rulesTypeCountMap = new HashMap<>();
        criteriaTypeCountMap.forEach((criteriaType, count) -> rulesTypeCountMap.put(criteriaType.name().toLowerCase(Locale.ROOT), count));

        queryRulesUsage.put(TOTAL_COUNT, response.queryPage().count());
        queryRulesUsage.put(TOTAL_RULE_COUNT, ruleStats.getSum());
        queryRulesUsage.put(MIN_RULE_COUNT, results.isEmpty() ? 0 : ruleStats.getMin());
        queryRulesUsage.put(MAX_RULE_COUNT, results.isEmpty() ? 0 : ruleStats.getMax());
        if (rulesTypeCountMap.isEmpty() == false) {
            queryRulesUsage.put(RULE_CRITERIA_TOTAL_COUNTS, rulesTypeCountMap);
        }
    }
}
