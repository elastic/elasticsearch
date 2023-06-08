/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Streams;
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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
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
    public static final String QUERY_RULES_CONCRETE_INDEX_NAME = ".query-rules-1";
    public static final String QUERY_RULES_INDEX_NAME_PATTERN = ".query-rules-*";
    private final Client clientWithOrigin;

    public QueryRulesIndexService(Client client, NamedWriteableRegistry namedWriteableRegistry) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
    }

    /**
     * Returns the {@link SystemIndexDescriptor} for the {@link QueryRuleset} system index.
     *
     * @return The {@link SystemIndexDescriptor} for the {@link QueryRuleset} system index.
     */
    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(QUERY_RULES_INDEX_NAME_PATTERN)
            .setPrimaryIndex(QUERY_RULES_CONCRETE_INDEX_NAME)
            .setDescription("Contains query ruleset configuration for query rules")
            .setMappings(getIndexMappings())
            .setSettings(getIndexSettings())
            .setAliasName(QUERY_RULES_ALIAS_NAME)
            .setVersionMetaKey("version")
            .setOrigin(ENT_SEARCH_ORIGIN)
            .setThreadPools(ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 100)
            .put("index.refresh_interval", "1s")
            .build();
    }

    private static XContentBuilder getIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Version.CURRENT.toString());
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

                            builder.startObject(QueryRuleCriteria.VALUE_FIELD.getPreferredName());
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
        clientWithOrigin.get(getRequest, new DelegatingIndexNotFoundActionListener<>(resourceName, listener, (l, getResponse) -> {
            if (getResponse.isExists() == false) {
                l.onFailure(new ResourceNotFoundException(resourceName));
                return;
            }
            final Map<String, Object> source = getResponse.getSource();

            final String id = getResponse.getId();
            @SuppressWarnings("unchecked")
            final List<QueryRule> rules = ((List<Map<String, Object>>) source.get("rules")).stream()
                .map(
                    rule -> new QueryRule(
                        (String) rule.get("rule_id"),
                        QueryRuleType.queryRuleType((String) rule.get("type")),
                        parseCriteria((List<Map<String, Object>>) rule.get("criteria")),
                        (Map<String, Object>) rule.get("actions")
                    )
                )
                .collect(Collectors.toList());
            final QueryRuleset res = new QueryRuleset(id, rules);
            l.onResponse(res);
        }));
    }

    private List<QueryRuleCriteria> parseCriteria(List<Map<String, Object>> rawCriteria) {
        List<QueryRuleCriteria> criteria = new ArrayList<>(rawCriteria.size());
        for (Map<String, Object> entry : rawCriteria) {
            criteria.add(
                new QueryRuleCriteria(
                    QueryRuleCriteria.CriteriaType.criteriaType((String) entry.get("type")),
                    (String) entry.get("metadata"),
                    entry.get("value")
                )
            );
        }
        return criteria;
    }

    /**
     * Creates or updates the {@link QueryRuleset} in the underlying index.
     *
     * @param queryRuleset The query ruleset object.
     * @param create If true, a query ruleset with the specified unique identifier must not already exist
     * @param listener The action listener to invoke on response/failure.
     */
    public void putQueryRuleset(QueryRuleset queryRuleset, boolean create, ActionListener<IndexResponse> listener) {
        try {
            DocWriteRequest.OpType opType = (create ? DocWriteRequest.OpType.CREATE : DocWriteRequest.OpType.INDEX);
            final IndexRequest indexRequest = new IndexRequest(QUERY_RULES_ALIAS_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(queryRuleset.id())
                .opType(opType)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(queryRuleset.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
            clientWithOrigin.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }

    }

    public void deleteQueryRuleset(String resourceName, ActionListener<DeleteResponse> listener) {
        try {
            final DeleteRequest deleteRequest = new DeleteRequest(QUERY_RULES_ALIAS_NAME).id(resourceName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            clientWithOrigin.delete(
                deleteRequest,
                new DelegatingIndexNotFoundActionListener<>(resourceName, listener, (l, deleteResponse) -> {
                    if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(resourceName));
                        return;
                    }
                    l.onResponse(deleteResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * List the {@link QueryRuleset} in ascending order of their ids.
     *
     * @param from From index to start the search from.
     * @param size The maximum number of {@link QueryRuleset}s to return.
     * @param listener The action listener to invoke on response/failure.
     *
     * TODO add total number of rules per ruleset - We can add this when implementing the List command.
     */
    public void listQueryRulesets(int from, int size, ActionListener<QueryRulesetResult> listener) {
        try {
            final SearchSourceBuilder source = new SearchSourceBuilder().from(from)
                .size(size)
                .query(new MatchAllQueryBuilder())
                .docValueField(QueryRuleset.ID_FIELD.getPreferredName())
                .storedFields(Collections.singletonList("_none_"))
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
        final List<String> rulesetIds = Arrays.stream(response.getHits().getHits())
            .map(hit -> (String) hit.getDocumentFields().get(QueryRuleset.ID_FIELD.getPreferredName()).getValue())
            .toList();
        return new QueryRulesetResult(rulesetIds, (int) response.getHits().getTotalHits().value);
    }

    static QueryRuleset parseQueryRulesetBinaryWithVersion(StreamInput in) throws IOException {
        TransportVersion version = TransportVersion.readVersion(in);
        assert version.onOrBefore(TransportVersion.CURRENT) : version + " >= " + TransportVersion.CURRENT;
        in.setTransportVersion(version);
        return new QueryRuleset(in);
    }

    static QueryRule parseQueryRuleBinaryWithVersion(StreamInput in) throws IOException {
        TransportVersion version = TransportVersion.readVersion(in);
        assert version.onOrBefore(TransportVersion.CURRENT) : version + " >= " + TransportVersion.CURRENT;
        in.setTransportVersion(version);
        return new QueryRule(in);
    }

    static void writeQueryRulesetBinaryWithVersion(QueryRuleset queryRuleset, OutputStream os, TransportVersion minTransportVersion)
        throws IOException {
        // do not close the output
        os = Streams.noCloseStream(os);
        TransportVersion.writeVersion(minTransportVersion, new OutputStreamStreamOutput(os));
        try (OutputStreamStreamOutput out = new OutputStreamStreamOutput(os)) {
            out.setTransportVersion(minTransportVersion);
            queryRuleset.writeTo(out);
        }
    }

    static void writeQueryRuleBinaryWithVersion(QueryRule queryRule, OutputStream os, TransportVersion minTransportVersion)
        throws IOException {
        // do not close the output
        os = Streams.noCloseStream(os);
        TransportVersion.writeVersion(minTransportVersion, new OutputStreamStreamOutput(os));
        try (OutputStreamStreamOutput out = new OutputStreamStreamOutput(os)) {
            out.setTransportVersion(minTransportVersion);
            queryRule.writeTo(out);
        }
    }

    static class DelegatingIndexNotFoundActionListener<T, R> extends DelegatingActionListener<T, R> {
        private final BiConsumer<ActionListener<R>, T> bc;
        private final String resourceName;

        DelegatingIndexNotFoundActionListener(String resourceName, ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
            super(delegate);
            this.bc = bc;
            this.resourceName = resourceName;
        }

        @Override
        public void onResponse(T t) {
            bc.accept(delegate, t);
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof IndexNotFoundException) {
                delegate.onFailure(new ResourceNotFoundException(resourceName, e));
                return;
            }
            delegate.onFailure(e);
        }
    }

    public record QueryRulesetResult(List<String> rulesetIds, long totalResults) {}
}
