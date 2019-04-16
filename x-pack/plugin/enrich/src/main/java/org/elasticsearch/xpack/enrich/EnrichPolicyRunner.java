/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import java.util.ArrayList;
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
import org.elasticsearch.common.collect.MapBuilder;
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

    EnrichPolicyRunner(ClusterService clusterService, Client client, EnrichStore enrichStore,
                              IndexNameExpressionResolver indexNameExpressionResolver,
                              LongSupplier nowSupplier) {
        this.clusterService = clusterService;
        this.client = client;
        this.enrichStore = enrichStore;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
    }

    public void runPolicy(final String policyId, final ActionListener<EnrichPolicyResult> listener) {
        // Look up policy in policy store and execute it
        runPolicy(policyId, enrichStore.getPolicy(policyId), listener);
    }

    public void runPolicy(final String policyName, final EnrichPolicy policy,
                          final ActionListener<EnrichPolicyResult> listener) {
        // TODO: Index Pattern expansion and Alias support
        // Collect the source index information
        logger.info("Policy [{}]: Running enrich policy", policyName);
        final String sourceIndexPattern = policy.getIndexPattern();
        logger.debug("Policy [{}]: Checking source index [{}]", policyName, sourceIndexPattern);
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(sourceIndexPattern);
        client.admin().indices().getIndex(getIndexRequest, new ActionListener<GetIndexResponse>() {
            @Override
            public void onResponse(GetIndexResponse getIndexResponse) {
                validateMappings(getIndexResponse, policyName, policy, listener);
                prepareAndCreateEnrichIndex(policyName, policy, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private Map<String, Object> getMappings(final GetIndexResponse getIndexResponse, final String sourceIndexName) {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.mappings();
        ImmutableOpenMap<String, MappingMetaData> indexMapping = mappings.get(sourceIndexName);
        assert indexMapping.keys().size() == 0 : "Expecting only one type per index";
        MappingMetaData typeMapping = indexMapping.iterator().next().value;
        return typeMapping.sourceAsMap();
    }

    private void validateMappings(final GetIndexResponse getIndexResponse, final String policyName, final EnrichPolicy policy,
                                  final ActionListener<?> listener) {
        String[] sourceIndices = getIndexResponse.getIndices();
        logger.debug("Policy [{}]: Validating [{}] source mappings", policyName, sourceIndices);
        for (String sourceIndex : sourceIndices) {
            Map<String, Object> mapping = getMappings(getIndexResponse, sourceIndex);
            Map<?, ?> properties = ((Map<?, ?>) mapping.get("properties"));
            if (properties == null) {
                listener.onFailure(
                    new ElasticsearchException(
                        "Enrich policy execution for [{}] failed. Could not read mapping for source [{}] included by pattern [{}]",
                        policyName, sourceIndex, policy.getIndexPattern()));
            }
            if (properties.containsKey(policy.getEnrichKey()) == false) {
                listener.onFailure(
                    new ElasticsearchException(
                        "Enrich policy execution for [{}] failed. Could not locate enrich key field [{}] on mapping for index [{}]",
                        policyName, policy.getEnrichKey(), sourceIndex));
            }
            for (String enrichField : policy.getEnrichValues()) {
                if (properties.containsKey(enrichField) == false) {
                    listener.onFailure(new ElasticsearchException(
                        "Enrich policy execution for [{}] failed. Could not locate enrich value field [{}] on mapping for index [{}]",
                        policyName, enrichField, sourceIndex));
                }
            }
        }
    }

    private String getEnrichIndexBase(final String policyName) {
        return ".enrich-" + policyName;
    }

    private Map<String, Object> resolveEnrichMapping(final EnrichPolicy policy) {
        // TODO: Modify enrichKeyFieldMapping based on policy type.
        // Create dynamic mapping templates - one for the enrich key and one for the enrich values
        Map<String, Object> enrichKeyFieldMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("type", "{dynamic_type}")
            .put("indexed", true)
            .put("doc_values", false)
            .map();

        Map<String, Object> enrichKeyTemplateDefinition = MapBuilder.<String, Object>newMapBuilder()
            .put("match", policy.getEnrichKey())
            .put("mapping", enrichKeyFieldMapping)
            .map();

        Map<String, Object> enrichKeyTemplate = MapBuilder.<String, Object>newMapBuilder()
            .put("enrich_key_template", enrichKeyTemplateDefinition)
            .map();

        Map<String, Object> enrichTextValueFieldMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("type", "keyword")
            .put("ignore_above", 256)
            .put("indexed", false)
            .put("doc_values", true)
            .map();

        Map<String, Object> enrichTextValueTemplateDefinition = MapBuilder.<String, Object>newMapBuilder()
            .put("match_mapping_type", "string")
            .put("match", "*")
            .put("unmatch", policy.getEnrichKey())
            .put("mapping", enrichTextValueFieldMapping)
            .map();

        Map<String, Object> enrichTextValueTemplate = MapBuilder.<String, Object>newMapBuilder()
            .put("enrich_text_value_template", enrichTextValueTemplateDefinition)
            .map();

        Map<String, Object> enrichValueFieldMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("type", "{dynamic_type}")
            .put("indexed", false)
            .put("doc_values", true)
            .map();

        Map<String, Object> enrichValueTemplateDefinition = MapBuilder.<String, Object>newMapBuilder()
            .put("match", "*")
            .put("unmatch", policy.getEnrichKey())
            .put("mapping", enrichValueFieldMapping)
            .map();

        Map<String, Object> enrichValueTemplate = MapBuilder.<String, Object>newMapBuilder()
            .put("enrich_value_template", enrichValueTemplateDefinition)
            .map();

        List<Map<String, Object>> templates = new ArrayList<>();
        templates.add(enrichKeyTemplate);
        templates.add(enrichTextValueTemplate);
        templates.add(enrichValueTemplate);

        return MapBuilder.<String, Object>newMapBuilder()
            .put("dynamic_templates", templates)
            .map();
    }

    private void prepareAndCreateEnrichIndex(final String policyName, final EnrichPolicy policy,
                                             final ActionListener<EnrichPolicyResult> listener) {
        long nowTimestamp = nowSupplier.getAsLong();
        String enrichIndexName = getEnrichIndexBase(policyName) + "-" + nowTimestamp;
        // TODO: Settings for localizing enrich indices to nodes that are ingest+data only
        Settings enrichIndexSettings = Settings.EMPTY;
        Map<String, Object> enrichMapping = resolveEnrichMapping(policy);
        CreateIndexRequest createEnrichIndexRequest = new CreateIndexRequest(enrichIndexName, enrichIndexSettings);
        createEnrichIndexRequest.mapping("_doc", enrichMapping);
        logger.debug("Policy [{}]: Creating new enrich index [{}]", policyName, enrichIndexName);
        client.admin().indices().create(createEnrichIndexRequest, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                transferDataToEnrichIndex(enrichIndexName, policyName, policy, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void transferDataToEnrichIndex(final String destinationIndexName, final String policyName, final EnrichPolicy policy,
                                           final ActionListener<EnrichPolicyResult> listener) {
        logger.debug("Policy [{}]: Transferring source data to new enrich index [{}]", policyName, destinationIndexName);
        // Filter down the source fields to just the ones required by the policy
        final Set<String> retainFields = new HashSet<>();
        retainFields.add(policy.getEnrichKey());
        retainFields.addAll(policy.getEnrichValues());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(retainFields.toArray(new String[0]), new String[0]);
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
                    refreshEnrichIndex(destinationIndexName, policyName, listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void refreshEnrichIndex(final String destinationIndexName, final String policyName,
                                    final ActionListener<EnrichPolicyResult> listener) {
        logger.debug("Policy [{}]: Refreshing newly created enrich index [{}]", policyName, destinationIndexName);
        client.admin().indices().refresh(new RefreshRequest(destinationIndexName), new ActionListener<RefreshResponse>() {
            @Override
            public void onResponse(RefreshResponse refreshResponse) {
                updateEnrichPolicyAlias(destinationIndexName, policyName, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void updateEnrichPolicyAlias(final String destinationIndexName, final String policyName,
                                         final ActionListener<EnrichPolicyResult> listener) {
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
