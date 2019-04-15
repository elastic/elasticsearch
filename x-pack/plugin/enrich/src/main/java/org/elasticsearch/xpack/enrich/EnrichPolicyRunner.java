/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class EnrichPolicyRunner {

    private static final Logger logger = LogManager.getLogger(EnrichPolicyRunner.class);

    private ClusterService clusterService;
    private Client client;
    private EnrichStore enrichStore;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private LongSupplier nowSupplier;

    public EnrichPolicyRunner(ClusterService clusterService, Client client, EnrichStore enrichStore,
                              IndexNameExpressionResolver indexNameExpressionResolver,
                              LongSupplier nowSupplier) {
        this.clusterService = clusterService;
        this.client = client;
        this.enrichStore = enrichStore;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
    }

    public void runPolicy(String policyId, ActionListener<EnrichPolicyResult> listener) throws Exception {
        // Look up policy in policy store and execute it
        runPolicy(policyId, enrichStore.getPolicy(policyId), listener);
    }

    public void runPolicy(final String policyName, final EnrichPolicy policy,
                          final ActionListener<EnrichPolicyResult> listener) throws Exception {
        // TODO: Index Pattern expansion and Alias support
        // Collect the source index information
        logger.info("Policy [{}]: Running enrich policy", policyName);
        final String sourceIndexPattern = policy.getIndexPattern();
        logger.debug("Policy [{}]: Checking source index [{}]", policyName, sourceIndexPattern);
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(sourceIndexPattern);
        client.admin().indices().getIndex(getIndexRequest, new ActionListener<GetIndexResponse>() {
            @Override
            public void onResponse(GetIndexResponse getIndexResponse) {
                validateMappings(getIndexResponse, sourceIndexPattern, policyName, policy, listener);
                prepareAndCreateEnrichIndex(getIndexResponse, sourceIndexPattern, policyName, policy, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private Map<String, Object> getMappings(GetIndexResponse getIndexResponse, String sourceIndexName) {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.mappings();
        ImmutableOpenMap<String, MappingMetaData> indexMapping = mappings.get(sourceIndexName);
        // Assuming single mapping
        MappingMetaData typeMapping = indexMapping.iterator().next().value;
        Map<String, Object> mapping = typeMapping.sourceAsMap();
        return mapping;
    }

    private void validateMappings(GetIndexResponse getIndexResponse, String sourceIndexPattern, String policyName, EnrichPolicy policy,
                                  ActionListener<?> listener) {
        logger.debug("Policy [{}]: Validating [{}] source mappings", policyName, sourceIndexPattern);
        Map<String, Object> mapping = getMappings(getIndexResponse, sourceIndexPattern);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = ((Map<String, Object>) mapping.get("properties"));
        if (properties == null) {
            listener.onFailure(
                new ElasticsearchException("Could not read mapping for source [{}]",
                    sourceIndexPattern));
        }
        if (properties.containsKey(policy.getEnrichKey()) == false) {
            listener.onFailure(
                new ElasticsearchException("Could not locate primary lookup field [{}] on source mapping for index [{}]",
                    policy.getEnrichKey(), sourceIndexPattern));
        }
        for (String enrichField : policy.getEnrichValues()) {
            if (properties.containsKey(enrichField) == false) {
                listener.onFailure(new ElasticsearchException("Could not locate enrichment field [{}] on source mapping for index [{}]",
                    enrichField, sourceIndexPattern));
            }
        }
    }

    private String getEnrichIndexBase(String policyName) {
        return ".enrich-" + policyName;
    }

    private Map<String, Object> resolveEnrichMapping(GetIndexResponse getIndexResponse, String sourceIndexName, Set<String> retainFields) {
        // TODO: Minimize the mappings here, merge mappings from multiple indices
        Map<String, Object> mapping = getMappings(getIndexResponse, sourceIndexName);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = ((Map<String, Object>) mapping.get("properties"));
        properties.keySet().retainAll(retainFields);
        return mapping;
    }

    private void prepareAndCreateEnrichIndex(GetIndexResponse getIndexResponse, String sourceIndexName, String policyName,
                                             EnrichPolicy policy, ActionListener<EnrichPolicyResult> listener) {
        long nowTimestamp = nowSupplier.getAsLong();
        String enrichIndexName = getEnrichIndexBase(policyName) + "-" + nowTimestamp;
        Settings enrichIndexSettings = Settings.EMPTY;
        final Set<String> retainFields = new HashSet<>();
        retainFields.add(policy.getEnrichKey());
        retainFields.addAll(policy.getEnrichValues());
        Map<String, Object> enrichMapping = resolveEnrichMapping(getIndexResponse, sourceIndexName, retainFields);
        CreateIndexRequest createEnrichIndexRequest = new CreateIndexRequest(enrichIndexName, enrichIndexSettings);
        createEnrichIndexRequest.mapping("_doc", enrichMapping);
        logger.debug("Policy [{}]: Creating new enrich index [{}]", policyName, enrichIndexName);
        client.admin().indices().create(createEnrichIndexRequest, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                transferDataToEnrichIndex(enrichIndexName, retainFields, policyName, policy, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void transferDataToEnrichIndex(String destinationIndexName, Set<String> includeFields, String policyName, EnrichPolicy policy,
                                           ActionListener<EnrichPolicyResult> listener) {
        logger.debug("Policy [{}]: Transferring source data to new enrich index [{}]", policyName, destinationIndexName);
        // Filter down the source fields to just the ones required by the policy
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(includeFields.toArray(new String[0]), new String[0]);
        if (policy.getQuery() != null) {
            searchSourceBuilder.query(QueryBuilders.wrapperQuery(policy.getQuery().getQuery()));
        } else {
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        }
        ReindexRequest reindexRequest = new ReindexRequest()
            .setDestIndex(destinationIndexName)
            .setSourceIndices(policy.getIndexPattern());
        reindexRequest.getSearchRequest().source(searchSourceBuilder);
        client.execute(ReindexAction.INSTANCE, reindexRequest, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                // Do we want to fail the request if there were failures during the reindex process?
                if (bulkByScrollResponse.getBulkFailures().size() > 0) {
                    listener.onFailure(new ElasticsearchException("Encountered bulk failures during reindex process"));
                } else if (bulkByScrollResponse.getSearchFailures().size() > 0) {
                    listener.onFailure(new ElasticsearchException("Encountered search failures during reindex process"));
                } else {
                    logger.info("Policy [{}]: Transferred [{}] documents to enrich index [{}]", policyName,
                        bulkByScrollResponse.getCreated(), destinationIndexName);
                    refreshEnrichIndex(destinationIndexName, policyName, policy, listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void refreshEnrichIndex(String destinationIndexName, String policyName, EnrichPolicy policy,
                                    ActionListener<EnrichPolicyResult> listener) {
        logger.debug("Policy [{}]: Refreshing newly created enrich index [{}]", policyName, destinationIndexName);
        client.admin().indices().refresh(new RefreshRequest(destinationIndexName), new ActionListener<RefreshResponse>() {
            @Override
            public void onResponse(RefreshResponse refreshResponse) {
                updateEnrichPolicyAlias(destinationIndexName, policyName, policy, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void updateEnrichPolicyAlias(String destinationIndexName, String policyName, EnrichPolicy policy,
                                         ActionListener<EnrichPolicyResult> listener) {
        String enrichIndexBase = getEnrichIndexBase(policyName);
        logger.debug("Policy [{}]: Promoting new enrich index [{}] to alias [{}]", policyName, destinationIndexName, enrichIndexBase);
        GetAliasesRequest aliasRequest = new GetAliasesRequest(enrichIndexBase);
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), aliasRequest);
        ImmutableOpenMap<String, List<AliasMetaData>> aliases =
            clusterService.state().metaData().findAliases(aliasRequest, concreteIndices);
        IndicesAliasesRequest aliasToggleRequest = new IndicesAliasesRequest();
        String[] indices = aliases.keys().toArray(String.class);
        if (indices.length > 0) {
            aliasToggleRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().indices(indices).alias(enrichIndexBase));
        }
        aliasToggleRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(destinationIndexName).alias(enrichIndexBase));
        client.admin().indices().aliases(aliasToggleRequest, new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                logger.info("Policy [{}]: Policy execution complete", policyName);
                listener.onResponse(new EnrichPolicyResult(true));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
