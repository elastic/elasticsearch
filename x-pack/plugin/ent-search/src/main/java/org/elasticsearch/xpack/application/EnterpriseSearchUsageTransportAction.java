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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction;
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
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.MAX_RULE_COUNT;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.MIN_RULE_COUNT;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.RULE_CRITERIA_TOTAL_COUNTS;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.TOTAL_COUNT;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.TOTAL_RULE_COUNT;

public class EnterpriseSearchUsageTransportAction extends XPackUsageFeatureTransportAction {
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
        Settings settings,
        XPackLicenseState licenseState,
        Client client
    ) {
        super(XPackUsageFeatureAction.ENTERPRISE_SEARCH.name(), transportService, clusterService, threadPool, actionFilters);
        this.licenseState = licenseState;
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.indicesAdminClient = clientWithOrigin.admin().indices();
        this.enabled = XPackSettings.ENTERPRISE_SEARCH_ENABLED.get(settings);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        if (enabled == false) {
            EnterpriseSearchFeatureSetUsage usage = new EnterpriseSearchFeatureSetUsage(
                LicenseUtils.PLATINUM_LICENSED_FEATURE.checkWithoutTracking(licenseState),
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
                        LicenseUtils.PLATINUM_LICENSED_FEATURE.checkWithoutTracking(licenseState),
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
                        LicenseUtils.PLATINUM_LICENSED_FEATURE.checkWithoutTracking(licenseState),
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
            request.masterTimeout(),
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
                        .map(indexShardStats -> indexShardStats.getPrimaries().getDocs())
                        .filter(Objects::nonNull)
                        .mapToInt(docsStats -> (int) docsStats.getCount())
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

        Map<String, Object> ruleCriteriaTypeCountMap = new HashMap<>();
        Map<String, Object> ruleTypeCountMap = new HashMap<>();

        results.forEach(result -> {
            populateCounts(ruleCriteriaTypeCountMap, result.criteriaTypeToCountMap());
            populateCounts(ruleTypeCountMap, result.ruleTypeToCountMap());
        });

        queryRulesUsage.put(TOTAL_COUNT, response.queryPage().count());
        queryRulesUsage.put(TOTAL_RULE_COUNT, ruleStats.getSum());
        queryRulesUsage.put(MIN_RULE_COUNT, results.isEmpty() ? 0 : ruleStats.getMin());
        queryRulesUsage.put(MAX_RULE_COUNT, results.isEmpty() ? 0 : ruleStats.getMax());
        if (ruleCriteriaTypeCountMap.isEmpty() == false) {
            queryRulesUsage.put(RULE_CRITERIA_TOTAL_COUNTS, ruleCriteriaTypeCountMap);
        }
        if (ruleTypeCountMap.isEmpty() == false) {
            queryRulesUsage.put(EnterpriseSearchFeatureSetUsage.RULE_TYPE_TOTAL_COUNTS, ruleTypeCountMap);
        }
    }

    private void populateCounts(Map<String, Object> targetMap, Map<? extends Enum<?>, Integer> sourceMap) {
        sourceMap.forEach(
            (key, value) -> targetMap.merge(key.name().toLowerCase(Locale.ROOT), value, (v1, v2) -> (Integer) v1 + (Integer) v2)
        );
    }
}
