/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicyDefinition;

import static org.elasticsearch.xpack.enrich.ExactMatchProcessor.ENRICH_KEY_FIELD_NAME;

public class EnrichPolicyRunner implements Runnable {

    private static final Logger logger = LogManager.getLogger(EnrichPolicyRunner.class);

    private final String policyName;
    private final EnrichPolicyDefinition policy;
    private final ActionListener<PolicyExecutionResult> listener;
    private final ClusterService clusterService;
    private final Client client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final LongSupplier nowSupplier;
    private final int fetchSize;

    EnrichPolicyRunner(String policyName, EnrichPolicyDefinition policy, ActionListener<PolicyExecutionResult> listener,
                       ClusterService clusterService, Client client, IndexNameExpressionResolver indexNameExpressionResolver,
                       LongSupplier nowSupplier, int fetchSize) {
        this.policyName = policyName;
        this.policy = policy;
        this.listener = listener;
        this.clusterService = clusterService;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
        this.fetchSize = fetchSize;
    }

    @Override
    public void run() {
        // Collect the source index information
        logger.info("Policy [{}]: Running enrich policy", policyName);
        final String[] sourceIndices = policy.getIndices().toArray(new String[0]);
        logger.debug("Policy [{}]: Checking source indices [{}]", policyName, sourceIndices);
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(sourceIndices);
        client.admin().indices().getIndex(getIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetIndexResponse getIndexResponse) {
                validateMappings(getIndexResponse);
                prepareAndCreateEnrichIndex();
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
        if (indexMapping.keys().size() == 0) {
            throw new ElasticsearchException(
                "Enrich policy execution for [{}] failed. No mapping available on source [{}] included in [{}]",
                policyName, sourceIndexName, policy.getIndices());
        }
        assert indexMapping.keys().size() == 1 : "Expecting only one type per index";
        MappingMetaData typeMapping = indexMapping.iterator().next().value;
        return typeMapping.sourceAsMap();
    }

    private void validateMappings(final GetIndexResponse getIndexResponse) {
        String[] sourceIndices = getIndexResponse.getIndices();
        logger.debug("Policy [{}]: Validating [{}] source mappings", policyName, sourceIndices);
        for (String sourceIndex : sourceIndices) {
            Map<String, Object> mapping = getMappings(getIndexResponse, sourceIndex);
            // First ensure mapping is set
            if (mapping.get("properties") == null) {
                throw new ElasticsearchException(
                    "Enrich policy execution for [{}] failed. Could not read mapping for source [{}] included by pattern [{}]",
                    policyName, sourceIndex, policy.getIndices());
            }
            // Validate the key and values
            try {
                validateField(mapping, policy.getEnrichKey(), true);
                for (String valueFieldName : policy.getEnrichValues()) {
                    validateField(mapping, valueFieldName, false);
                }
            } catch (ElasticsearchException e) {
                throw new ElasticsearchException(
                        "Enrich policy execution for [{}] failed while validating field mappings for index [{}]",
                        e, policyName, sourceIndex);
            }
        }
    }

    private void validateField(Map<?, ?> properties, String fieldName, boolean fieldRequired) {
        assert Strings.isEmpty(fieldName) == false: "Field name cannot be null or empty";
        String[] fieldParts = fieldName.split("\\.");
        StringBuilder parent = new StringBuilder();
        Map<?, ?> currentField = properties;
        boolean onRoot = true;
        for (String fieldPart : fieldParts) {
            // Ensure that the current field is of object type only (not a nested type or a non compound field)
            Object type = currentField.get("type");
            if (type != null && "object".equals(type) == false) {
                throw new ElasticsearchException(
                    "Could not traverse mapping to field [{}]. The [{}] field must be regular object but was [{}].",
                    fieldName,
                    onRoot ? "root" : parent.toString(),
                    type
                );
            }
            Map<?, ?> currentProperties = ((Map<?, ?>) currentField.get("properties"));
            if (currentProperties == null) {
                if (fieldRequired) {
                    throw new ElasticsearchException(
                        "Could not traverse mapping to field [{}]. Expected the [{}] field to have sub fields but none were configured.",
                        fieldName,
                        onRoot ? "root" : parent.toString()
                    );
                } else {
                    return;
                }
            }
            currentField = ((Map<?, ?>) currentProperties.get(fieldPart));
            if (currentField == null) {
                if (fieldRequired) {
                    throw new ElasticsearchException(
                        "Could not traverse mapping to field [{}]. Could not find the [{}] field under [{}]",
                        fieldName,
                        fieldPart,
                        onRoot ? "root" : parent.toString()
                    );
                } else {
                    return;
                }
            }
            if (onRoot) {
               onRoot = false;
            } else {
                parent.append(".");
            }
            parent.append(fieldPart);
        }
    }

    private XContentBuilder resolveEnrichMapping(final EnrichPolicyDefinition policy) {
        // Currently the only supported policy type is EnrichPolicyDefinition.EXACT_MATCH_TYPE, which is a keyword type
        String keyType;
        if (EnrichPolicyDefinition.EXACT_MATCH_TYPE.equals(policy.getType())) {
            keyType = "keyword";
        } else {
            throw new ElasticsearchException("Unrecognized enrich policy type [{}]", policy.getType());
        }

        // Disable _source on enrich index. Explicitly mark key mapping type.
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .field("dynamic", false)
                    .startObject("_source")
                        .field("enabled", true)
                    .endObject()
                    .startObject("properties")
                        .startObject(policy.getEnrichKey())
                            .field("type", keyType)
                            .field("doc_values", false)
                        .endObject()
                    .endObject()
                    .startObject("_meta")
                        .field(ENRICH_KEY_FIELD_NAME, policy.getEnrichKey())
                    .endObject()
                .endObject()
            .endObject();

            return builder;
        } catch (IOException ioe) {
            throw new UncheckedIOException("Could not render enrich mapping", ioe);
        }
    }

    private void prepareAndCreateEnrichIndex() {
        long nowTimestamp = nowSupplier.getAsLong();
        String enrichIndexName = EnrichPolicyDefinition.getBaseName(policyName) + "-" + nowTimestamp;
        Settings enrichIndexSettings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .build();
        CreateIndexRequest createEnrichIndexRequest = new CreateIndexRequest(enrichIndexName, enrichIndexSettings);
        createEnrichIndexRequest.mapping(MapperService.SINGLE_MAPPING_NAME, resolveEnrichMapping(policy));
        logger.debug("Policy [{}]: Creating new enrich index [{}]", policyName, enrichIndexName);
        client.admin().indices().create(createEnrichIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                transferDataToEnrichIndex(enrichIndexName);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void transferDataToEnrichIndex(final String destinationIndexName) {
        logger.debug("Policy [{}]: Transferring source data to new enrich index [{}]", policyName, destinationIndexName);
        // Filter down the source fields to just the ones required by the policy
        final Set<String> retainFields = new HashSet<>();
        retainFields.add(policy.getEnrichKey());
        retainFields.addAll(policy.getEnrichValues());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(fetchSize);
        searchSourceBuilder.fetchSource(retainFields.toArray(new String[0]), new String[0]);
        if (policy.getQuery() != null) {
            searchSourceBuilder.query(QueryBuilders.wrapperQuery(policy.getQuery().getQuery()));
        }
        ReindexRequest reindexRequest = new ReindexRequest()
            .setDestIndex(destinationIndexName)
            .setSourceIndices(policy.getIndices().toArray(new String[0]));
        reindexRequest.getSearchRequest().source(searchSourceBuilder);
        reindexRequest.getDestination().source(new BytesArray(new byte[0]), XContentType.SMILE);
        client.execute(ReindexAction.INSTANCE, reindexRequest, new ActionListener<>() {
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
                    forceMergeEnrichIndex(destinationIndexName);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void forceMergeEnrichIndex(final String destinationIndexName) {
        logger.debug("Policy [{}]: Force merging newly created enrich index [{}]", policyName, destinationIndexName);
        client.admin().indices().forceMerge(new ForceMergeRequest(destinationIndexName).maxNumSegments(1), new ActionListener<>() {
            @Override
            public void onResponse(ForceMergeResponse forceMergeResponse) {
                refreshEnrichIndex(destinationIndexName);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void refreshEnrichIndex(final String destinationIndexName) {
        logger.debug("Policy [{}]: Refreshing newly created enrich index [{}]", policyName, destinationIndexName);
        client.admin().indices().refresh(new RefreshRequest(destinationIndexName), new ActionListener<>() {
            @Override
            public void onResponse(RefreshResponse refreshResponse) {
                setIndexReadOnly(destinationIndexName);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void setIndexReadOnly(final String destinationIndexName) {
        logger.debug("Policy [{}]: Setting new enrich index [{}] to be read only", policyName, destinationIndexName);
        UpdateSettingsRequest request = new UpdateSettingsRequest(destinationIndexName)
            .setPreserveExisting(true)
            .settings(Settings.builder()
                .put("index.auto_expand_replicas", "0-all")
                .put("index.blocks.write", "true"));
        client.admin().indices().updateSettings(request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                waitForIndexGreen(destinationIndexName);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void waitForIndexGreen(final String destinationIndexName) {
        ClusterHealthRequest request = new ClusterHealthRequest(destinationIndexName).waitForGreenStatus();
        client.admin().cluster().health(request, new ActionListener<>() {
            @Override
            public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                updateEnrichPolicyAlias(destinationIndexName);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void updateEnrichPolicyAlias(final String destinationIndexName) {
        String enrichIndexBase = EnrichPolicyDefinition.getBaseName(policyName);
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
        client.admin().indices().aliases(aliasToggleRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                logger.info("Policy [{}]: Policy execution complete", policyName);
                listener.onResponse(new PolicyExecutionResult(true));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
