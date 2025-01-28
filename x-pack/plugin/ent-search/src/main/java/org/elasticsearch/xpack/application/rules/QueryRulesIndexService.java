/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.rules.action.DeleteQueryRuleAction;
import org.elasticsearch.xpack.application.rules.action.PutQueryRuleAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.application.rules.QueryRule.QueryRuleType;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

/**
 * A service that manages persistent {@link QueryRuleset} configurations.
 */
public class QueryRulesIndexService {
    private static final Logger logger = LogManager.getLogger(QueryRulesIndexService.class);
    public static final String QUERY_RULES_ALIAS_NAME = ".query-rules";
    public static final String QUERY_RULES_INDEX_PREFIX = ".query-rules-";
    public static final String QUERY_RULES_CONCRETE_INDEX_NAME = QUERY_RULES_INDEX_PREFIX + QueryRulesIndexMappingVersion.latest().id;
    public static final String QUERY_RULES_INDEX_NAME_PATTERN = ".query-rules-*";
    private final Client clientWithOrigin;
    private final ClusterSettings clusterSettings;

    public QueryRulesIndexService(Client client, ClusterSettings clusterSettings) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.clusterSettings = clusterSettings;
    }

    /**
     * Returns the {@link SystemIndexDescriptor} for the {@link QueryRuleset} system index.
     *
     * @return The {@link SystemIndexDescriptor} for the {@link QueryRuleset} system index.
     */
    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        final Function<QueryRulesIndexMappingVersion, SystemIndexDescriptor.Builder> systemIndexDescriptorBuilder =
            mappingVersion -> SystemIndexDescriptor.builder()
                .setIndexPattern(QUERY_RULES_INDEX_NAME_PATTERN)
                .setPrimaryIndex(QUERY_RULES_CONCRETE_INDEX_NAME)
                .setDescription("Contains query ruleset configuration for query rules")
                .setMappings(getIndexMappings(mappingVersion))
                .setSettings(getIndexSettings())
                .setAliasName(QUERY_RULES_ALIAS_NAME)
                .setIndexFormat(QueryRulesIndexMappingVersion.latest().id)
                .setOrigin(ENT_SEARCH_ORIGIN)
                .setThreadPools(ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS);

        return systemIndexDescriptorBuilder.apply(QueryRulesIndexMappingVersion.latest())
            .setPriorSystemIndexDescriptors(List.of(systemIndexDescriptorBuilder.apply(QueryRulesIndexMappingVersion.INITIAL).build()))
            .build();
    }

    private static Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 100)
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), QueryRulesIndexMappingVersion.latest().id)
            .put("index.refresh_interval", "1s")
            .build();
    }

    private static XContentBuilder getIndexMappings(QueryRulesIndexMappingVersion version) {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Version.CURRENT.toString());
                builder.field(SystemIndexDescriptor.VERSION_META_KEY, version.id);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject(QueryRuleset.ID_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(QueryRuleset.RULES_FIELD.getPreferredName());
                    builder.startObject("properties");
                    {
                        builder.startObject(QueryRule.ID_FIELD.getPreferredName());
                        builder.field("type", "keyword");
                        builder.endObject();

                        builder.startObject(QueryRule.TYPE_FIELD.getPreferredName());
                        builder.field("type", "keyword");
                        builder.endObject();

                        builder.startObject(QueryRule.CRITERIA_FIELD.getPreferredName());
                        builder.startObject("properties");
                        {
                            builder.startObject(QueryRuleCriteria.TYPE_FIELD.getPreferredName());
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject(QueryRuleCriteria.METADATA_FIELD.getPreferredName());
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject(QueryRuleCriteria.VALUES_FIELD.getPreferredName());
                            builder.field("type", "object");
                            builder.field("enabled", false);
                            builder.endObject();
                        }
                        builder.endObject();
                        builder.endObject();

                        builder.startObject(QueryRule.ACTIONS_FIELD.getPreferredName());
                        builder.field("type", "object");
                        builder.field("enabled", false);
                        builder.endObject();

                        if (version.onOrAfter(QueryRulesIndexMappingVersion.ADD_PRIORITY)) {
                            builder.startObject(QueryRule.PRIORITY_FIELD.getPreferredName());
                            builder.field("type", "integer");
                            builder.endObject();
                        }
                    }
                    builder.endObject();
                    builder.endObject();

                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build " + QUERY_RULES_CONCRETE_INDEX_NAME + " index mappings", e);
            throw new UncheckedIOException("Failed to build " + QUERY_RULES_CONCRETE_INDEX_NAME + " index mappings", e);
        }
    }

    /**
     * Gets the {@link QueryRuleset} from the index if present, or delegate a {@link ResourceNotFoundException} failure to the provided
     * listener if not.
     *
     * @param resourceName The resource name.
     * @param listener The action listener to invoke on response/failure.
     */
    public void getQueryRuleset(String resourceName, ActionListener<QueryRuleset> listener) {
        final GetRequest getRequest = new GetRequest(QUERY_RULES_ALIAS_NAME).id(resourceName).realtime(true);

        clientWithOrigin.get(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    listener.onFailure(new ResourceNotFoundException(resourceName));
                    return;
                }
                final Map<String, Object> source = getResponse.getSource();
                @SuppressWarnings("unchecked")
                final List<QueryRule> rules = ((List<Map<String, Object>>) source.get(QueryRuleset.RULES_FIELD.getPreferredName())).stream()
                    .map(
                        rule -> new QueryRule(
                            (String) rule.get(QueryRule.ID_FIELD.getPreferredName()),
                            QueryRuleType.queryRuleType((String) rule.get(QueryRule.TYPE_FIELD.getPreferredName())),
                            parseCriteria((List<Map<String, Object>>) rule.get(QueryRule.CRITERIA_FIELD.getPreferredName())),
                            (Map<String, Object>) rule.get(QueryRule.ACTIONS_FIELD.getPreferredName()),
                            (Integer) rule.get(QueryRule.PRIORITY_FIELD.getPreferredName())
                        )
                    )
                    .collect(Collectors.toList());
                final QueryRuleset res = new QueryRuleset(resourceName, rules);
                listener.onResponse(res);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException(resourceName));
                    return;
                }
                listener.onFailure(e);
            }
        });
    }

    /**
     * Retrieves a {@link QueryRule} from a {@link QueryRuleset}.
     *
     * @param rulesetId
     * @param ruleId
     * @param listener
     */
    public void getQueryRule(String rulesetId, String ruleId, ActionListener<QueryRule> listener) {
        getQueryRuleset(rulesetId, listener.delegateFailure((delegate, queryRuleset) -> {
            Optional<QueryRule> maybeQueryRule = queryRuleset.rules().stream().filter(r -> r.id().equals(ruleId)).findFirst();
            if (maybeQueryRule.isPresent()) {
                delegate.onResponse(maybeQueryRule.get());
            } else {
                delegate.onFailure(new ResourceNotFoundException("rule id " + ruleId + " not found in ruleset " + rulesetId));
            }
        }));
    }

    @SuppressWarnings("unchecked")
    private static List<QueryRuleCriteria> parseCriteria(List<Map<String, Object>> rawCriteria) {
        List<QueryRuleCriteria> criteria = new ArrayList<>(rawCriteria.size());
        for (Map<String, Object> entry : rawCriteria) {
            criteria.add(
                new QueryRuleCriteria(
                    QueryRuleCriteriaType.type((String) entry.get(QueryRuleCriteria.TYPE_FIELD.getPreferredName())),
                    (String) entry.get(QueryRuleCriteria.METADATA_FIELD.getPreferredName()),
                    (List<Object>) entry.get(QueryRuleCriteria.VALUES_FIELD.getPreferredName())
                )
            );
        }
        return criteria;
    }

    /**
     * Creates or updates the {@link QueryRuleset} in the underlying index.
     *
     * @param queryRuleset The query ruleset object.
     * @param listener The action listener to invoke on response/failure.
     */
    public void putQueryRuleset(QueryRuleset queryRuleset, ActionListener<DocWriteResponse> listener) {
        try {
            validateQueryRuleset(queryRuleset);
            final IndexRequest indexRequest = new IndexRequest(QUERY_RULES_ALIAS_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(queryRuleset.id())
                .opType(DocWriteRequest.OpType.INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(queryRuleset.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
            clientWithOrigin.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Creates or updates a {@link QueryRule} within a {@link QueryRuleset} with the given {@code queryRulesetId}.
     * If the {@code queryRulesetId} is not associated with an existing {@link QueryRuleset}, a new {@link QueryRuleset} is created.
     * @param queryRulesetId
     * @param queryRule
     * @param listener
     */
    public void putQueryRule(String queryRulesetId, QueryRule queryRule, ActionListener<PutQueryRuleAction.Response> listener) {
        getQueryRuleset(queryRulesetId, new ActionListener<>() {
            @Override
            public void onResponse(QueryRuleset queryRuleset) {
                final List<QueryRule> rules = new ArrayList<>(queryRuleset.rules()).stream()
                    .filter(rule -> rule.id().equals(queryRule.id()) == false)
                    .collect(Collectors.toList());
                rules.add(queryRule);
                final boolean created = queryRuleset.rules().stream().noneMatch(rule -> rule.id().equals(queryRule.id()));

                putQueryRuleset(new QueryRuleset(queryRulesetId, rules), listener.delegateFailureAndWrap((delegate, docWriteResponse) -> {
                    DocWriteResponse.Result result = created ? DocWriteResponse.Result.CREATED : docWriteResponse.getResult();
                    delegate.onResponse(new PutQueryRuleAction.Response(result));
                }));
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ResourceNotFoundException) {
                    putQueryRuleset(
                        new QueryRuleset(queryRulesetId, List.of(queryRule)),
                        listener.delegateFailureAndWrap((delegate, docWriteResponse) -> {
                            delegate.onResponse(new PutQueryRuleAction.Response(DocWriteResponse.Result.CREATED));
                        })
                    );
                    return;
                }
                listener.onFailure(e);
            }
        });
    }

    private void validateQueryRuleset(QueryRuleset queryRuleset) {
        @SuppressWarnings("unchecked")
        Setting<Integer> maxRuleLimitSetting = (Setting<Integer>) clusterSettings.get(QueryRulesConfig.MAX_RULE_LIMIT_SETTING.getKey());
        int maxRuleLimit = clusterSettings.get(Objects.requireNonNull(maxRuleLimitSetting));
        if (queryRuleset.rules().size() > maxRuleLimit) {
            throw new IllegalArgumentException(
                "The number of rules in a ruleset cannot exceed ["
                    + maxRuleLimit
                    + "]."
                    + "This maximum can be set by changing the ["
                    + QueryRulesConfig.MAX_RULE_LIMIT_SETTING.getKey()
                    + "] setting."
            );
        }
    }

    public void deleteQueryRuleset(String resourceName, ActionListener<DeleteResponse> listener) {
        final DeleteRequest deleteRequest = new DeleteRequest(QUERY_RULES_ALIAS_NAME).id(resourceName)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        clientWithOrigin.delete(deleteRequest, new ActionListener<>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    listener.onFailure(new ResourceNotFoundException(resourceName));
                    return;
                }
                listener.onResponse(deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException(resourceName));
                    return;
                }
                listener.onFailure(e);
            }
        });
    }

    /**
     * Deletes a {@link QueryRule} from a {@link QueryRuleset}.
     *
     * @param rulesetId
     * @param ruleId
     * @param listener
     */
    public void deleteQueryRule(String rulesetId, String ruleId, ActionListener<DeleteQueryRuleAction.Response> listener) {
        getQueryRuleset(rulesetId, listener.delegateFailure((delegate, queryRuleset) -> {
            Optional<QueryRule> maybeQueryRule = queryRuleset.rules().stream().filter(r -> r.id().equals(ruleId)).findFirst();
            if (maybeQueryRule.isPresent()) {
                final List<QueryRule> rules = queryRuleset.rules()
                    .stream()
                    .filter(rule -> rule.id().equals(ruleId) == false)
                    .collect(Collectors.toList());
                if (rules.isEmpty() == false) {
                    putQueryRuleset(new QueryRuleset(rulesetId, rules), listener.delegateFailureAndWrap((delegate1, docWriteResponse) -> {
                        delegate1.onResponse(new DeleteQueryRuleAction.Response(true));
                    }));
                } else {
                    // Delete entire ruleset when there are no more rules left in it
                    deleteQueryRuleset(rulesetId, listener.delegateFailureAndWrap((delegate1, deleteResponse) -> {
                        delegate1.onResponse(new DeleteQueryRuleAction.Response(true));
                    }));
                }
            } else {
                delegate.onFailure(new ResourceNotFoundException("rule id " + ruleId + " not found in ruleset " + rulesetId));
            }
        }));
    }

    /**
     * List the {@link QueryRuleset} in ascending order of their ids.
     *
     * @param from From index to start the search from.
     * @param size The maximum number of {@link QueryRuleset}s to return.
     * @param listener The action listener to invoke on response/failure.
     */
    public void listQueryRulesets(int from, int size, ActionListener<QueryRulesetResult> listener) {
        try {
            final SearchSourceBuilder source = new SearchSourceBuilder().from(from)
                .size(size)
                .query(new MatchAllQueryBuilder())
                .fetchSource(
                    new String[] {
                        QueryRuleset.ID_FIELD.getPreferredName(),
                        QueryRuleset.RULES_FIELD.getPreferredName(),
                        QueryRuleset.RULES_FIELD.getPreferredName() + "." + QueryRule.TYPE_FIELD.getPreferredName() },
                    null
                )
                .sort(QueryRuleset.ID_FIELD.getPreferredName(), SortOrder.ASC);
            final SearchRequest req = new SearchRequest(QUERY_RULES_ALIAS_NAME).source(source);
            clientWithOrigin.search(req, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    listener.onResponse(mapSearchResponseToQueryRulesetList(searchResponse));
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IndexNotFoundException) {
                        listener.onResponse(new QueryRulesetResult(Collections.emptyList(), 0L));
                        return;
                    }
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static QueryRulesetResult mapSearchResponseToQueryRulesetList(SearchResponse response) {
        final List<QueryRulesetListItem> rulesetResults = Arrays.stream(response.getHits().getHits())
            .map(searchHit -> QueryRulesIndexService.hitToQueryRulesetListItem(searchHit.getSourceAsMap()))
            .toList();
        return new QueryRulesetResult(rulesetResults, (int) response.getHits().getTotalHits().value());
    }

    private static QueryRulesetListItem hitToQueryRulesetListItem(final Map<String, Object> sourceMap) {
        final String rulesetId = (String) sourceMap.get(QueryRuleset.ID_FIELD.getPreferredName());
        @SuppressWarnings("unchecked")
        final List<LinkedHashMap<?, ?>> rules = ((List<LinkedHashMap<?, ?>>) sourceMap.get(QueryRuleset.RULES_FIELD.getPreferredName()));
        final int numRules = rules.size();
        final Map<QueryRuleCriteriaType, Integer> queryRuleCriteriaTypeToCountMap = new EnumMap<>(QueryRuleCriteriaType.class);
        final Map<QueryRule.QueryRuleType, Integer> ruleTypeToCountMap = new EnumMap<>(QueryRule.QueryRuleType.class);
        for (LinkedHashMap<?, ?> rule : rules) {
            @SuppressWarnings("unchecked")
            List<LinkedHashMap<?, ?>> criteriaList = ((List<LinkedHashMap<?, ?>>) rule.get(QueryRule.CRITERIA_FIELD.getPreferredName()));
            for (LinkedHashMap<?, ?> criteria : criteriaList) {
                final String criteriaType = ((String) criteria.get(QueryRuleCriteria.TYPE_FIELD.getPreferredName()));
                final QueryRuleCriteriaType queryRuleCriteriaType = QueryRuleCriteriaType.type(criteriaType);
                queryRuleCriteriaTypeToCountMap.compute(queryRuleCriteriaType, (k, v) -> v == null ? 1 : v + 1);
            }
            final String ruleType = ((String) rule.get(QueryRule.TYPE_FIELD.getPreferredName()));
            final QueryRule.QueryRuleType queryRuleType = QueryRule.QueryRuleType.queryRuleType(ruleType);
            ruleTypeToCountMap.compute(queryRuleType, (k, v) -> v == null ? 1 : v + 1);
        }

        return new QueryRulesetListItem(rulesetId, numRules, queryRuleCriteriaTypeToCountMap, ruleTypeToCountMap);
    }

    public record QueryRulesetResult(List<QueryRulesetListItem> rulesets, long totalResults) {}

    public enum QueryRulesIndexMappingVersion implements VersionId<QueryRulesIndexMappingVersion> {
        INITIAL(1),
        ADD_PRIORITY(2),;

        private static final QueryRulesIndexMappingVersion LATEST = Arrays.stream(values())
            .max(Comparator.comparingInt(v -> v.id))
            .orElseThrow();

        private final int id;

        QueryRulesIndexMappingVersion(int id) {
            this.id = id;
        }

        @Override
        public int id() {
            return id;
        }

        public static QueryRulesIndexMappingVersion latest() {
            return LATEST;
        }
    }
}
