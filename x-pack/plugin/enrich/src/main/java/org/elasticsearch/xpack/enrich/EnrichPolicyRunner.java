/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

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
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
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
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

public class EnrichPolicyRunner implements Runnable {

    private static final Logger logger = LogManager.getLogger(EnrichPolicyRunner.class);

    static final String ENRICH_POLICY_NAME_FIELD_NAME = "enrich_policy_name";
    static final String ENRICH_POLICY_TYPE_FIELD_NAME = "enrich_policy_type";
    static final String ENRICH_MATCH_FIELD_NAME = "enrich_match_field";
    static final String ENRICH_README_FIELD_NAME = "enrich_readme";

    static final String ENRICH_INDEX_README_TEXT = "This index is managed by Elasticsearch and should not be modified in any way.";

    private final String policyName;
    private final EnrichPolicy policy;
    private final ExecuteEnrichPolicyTask task;
    private final ActionListener<ExecuteEnrichPolicyStatus> listener;
    private final ClusterService clusterService;
    private final Client client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final LongSupplier nowSupplier;
    private final int fetchSize;
    private final int maxForceMergeAttempts;

    EnrichPolicyRunner(
        String policyName,
        EnrichPolicy policy,
        ExecuteEnrichPolicyTask task,
        ActionListener<ExecuteEnrichPolicyStatus> listener,
        ClusterService clusterService,
        Client client,
        IndexNameExpressionResolver indexNameExpressionResolver,
        LongSupplier nowSupplier,
        int fetchSize,
        int maxForceMergeAttempts
    ) {
        this.policyName = policyName;
        this.policy = policy;
        this.task = task;
        this.listener = listener;
        this.clusterService = clusterService;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
        this.fetchSize = fetchSize;
        this.maxForceMergeAttempts = maxForceMergeAttempts;
    }

    @Override
    public void run() {
        logger.info("Policy [{}]: Running enrich policy", policyName);
        task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.RUNNING));
        // Collect the source index information
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
        ImmutableOpenMap<String, MappingMetaData> mappings = getIndexResponse.mappings();
        MappingMetaData indexMapping = mappings.get(sourceIndexName);
        if (indexMapping == MappingMetaData.EMPTY_MAPPINGS) {
            throw new ElasticsearchException(
                "Enrich policy execution for [{}] failed. No mapping available on source [{}] included in [{}]",
                policyName,
                sourceIndexName,
                policy.getIndices()
            );
        }
        return indexMapping.sourceAsMap();
    }

    private void validateMappings(final GetIndexResponse getIndexResponse) {
        String[] sourceIndices = getIndexResponse.getIndices();
        logger.debug("Policy [{}]: Validating [{}] source mappings", policyName, sourceIndices);
        for (String sourceIndex : sourceIndices) {
            Map<String, Object> mapping = getMappings(getIndexResponse, sourceIndex);
            validateMappings(policyName, policy, sourceIndex, mapping);
        }
    }

    static void validateMappings(
        final String policyName,
        final EnrichPolicy policy,
        final String sourceIndex,
        final Map<String, Object> mapping
    ) {
        // First ensure mapping is set
        if (mapping.get("properties") == null) {
            throw new ElasticsearchException(
                "Enrich policy execution for [{}] failed. Could not read mapping for source [{}] included by pattern [{}]",
                policyName,
                sourceIndex,
                policy.getIndices()
            );
        }
        // Validate the key and values
        try {
            validateField(mapping, policy.getMatchField(), true);
            for (String valueFieldName : policy.getEnrichFields()) {
                validateField(mapping, valueFieldName, false);
            }
        } catch (ElasticsearchException e) {
            throw new ElasticsearchException(
                "Enrich policy execution for [{}] failed while validating field mappings for index [{}]",
                e,
                policyName,
                sourceIndex
            );
        }
    }

    private static void validateField(Map<?, ?> properties, String fieldName, boolean fieldRequired) {
        assert Strings.isEmpty(fieldName) == false : "Field name cannot be null or empty";
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

    private XContentBuilder resolveEnrichMapping(final EnrichPolicy policy) {
        // Currently the only supported policy type is EnrichPolicy.MATCH_TYPE, which is a keyword type
        final String keyType;
        final CheckedFunction<XContentBuilder, XContentBuilder, IOException> matchFieldMapping;
        if (EnrichPolicy.MATCH_TYPE.equals(policy.getType())) {
            matchFieldMapping = (builder) -> builder.field("type", "keyword").field("doc_values", false);
            // No need to also configure index_options, because keyword type defaults to 'docs'.
        } else if (EnrichPolicy.GEO_MATCH_TYPE.equals(policy.getType())) {
            matchFieldMapping = (builder) -> builder.field("type", "geo_shape");
        } else {
            throw new ElasticsearchException("Unrecognized enrich policy type [{}]", policy.getType());
        }

        // Enable _source on enrich index. Explicitly mark key mapping type.
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startObject(MapperService.SINGLE_MAPPING_NAME);
                {
                    builder.field("dynamic", false);
                    builder.startObject("_source");
                    {
                        builder.field("enabled", true);
                    }
                    builder.endObject();
                    builder.startObject("properties");
                    {
                        builder.startObject(policy.getMatchField());
                        matchFieldMapping.apply(builder);
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject("_meta");
                    {
                        builder.field(ENRICH_README_FIELD_NAME, ENRICH_INDEX_README_TEXT);
                        builder.field(ENRICH_POLICY_NAME_FIELD_NAME, policyName);
                        builder.field(ENRICH_MATCH_FIELD_NAME, policy.getMatchField());
                        builder.field(ENRICH_POLICY_TYPE_FIELD_NAME, policy.getType());
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException ioe) {
            throw new UncheckedIOException("Could not render enrich mapping", ioe);
        }
    }

    private void prepareAndCreateEnrichIndex() {
        long nowTimestamp = nowSupplier.getAsLong();
        String enrichIndexName = EnrichPolicy.getBaseName(policyName) + "-" + nowTimestamp;
        Settings enrichIndexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            // No changes will be made to an enrich index after policy execution, so need to enable automatic refresh interval:
            .put("index.refresh_interval", -1)
            // This disables eager global ordinals loading for all fields:
            .put("index.warmer.enabled", false)
            .build();
        CreateIndexRequest createEnrichIndexRequest = new CreateIndexRequest(enrichIndexName, enrichIndexSettings);
        createEnrichIndexRequest.mapping(resolveEnrichMapping(policy));
        logger.debug("Policy [{}]: Creating new enrich index [{}]", policyName, enrichIndexName);
        client.admin().indices().create(createEnrichIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                prepareReindexOperation(enrichIndexName);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void prepareReindexOperation(final String destinationIndexName) {
        // Check to make sure that the enrich pipeline exists, and create it if it is missing.
        if (EnrichPolicyReindexPipeline.exists(clusterService.state()) == false) {
            EnrichPolicyReindexPipeline.create(client, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    transferDataToEnrichIndex(destinationIndexName);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } else {
            transferDataToEnrichIndex(destinationIndexName);
        }
    }

    private void transferDataToEnrichIndex(final String destinationIndexName) {
        logger.debug("Policy [{}]: Transferring source data to new enrich index [{}]", policyName, destinationIndexName);
        // Filter down the source fields to just the ones required by the policy
        final Set<String> retainFields = new HashSet<>();
        retainFields.add(policy.getMatchField());
        retainFields.addAll(policy.getEnrichFields());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(fetchSize);
        searchSourceBuilder.fetchSource(retainFields.toArray(new String[0]), new String[0]);
        if (policy.getQuery() != null) {
            searchSourceBuilder.query(QueryBuilders.wrapperQuery(policy.getQuery().getQuery()));
        }
        ReindexRequest reindexRequest = new ReindexRequest().setDestIndex(destinationIndexName)
            .setSourceIndices(policy.getIndices().toArray(new String[0]));
        reindexRequest.getSearchRequest().source(searchSourceBuilder);
        reindexRequest.getDestination().source(new BytesArray(new byte[0]), XContentType.SMILE);
        reindexRequest.getDestination().routing("discard");
        reindexRequest.getDestination().setPipeline(EnrichPolicyReindexPipeline.pipelineName());
        client.execute(ReindexAction.INSTANCE, reindexRequest, new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                // Do we want to fail the request if there were failures during the reindex process?
                if (bulkByScrollResponse.getBulkFailures().size() > 0) {
                    listener.onFailure(new ElasticsearchException("Encountered bulk failures during reindex process"));
                } else if (bulkByScrollResponse.getSearchFailures().size() > 0) {
                    listener.onFailure(new ElasticsearchException("Encountered search failures during reindex process"));
                } else {
                    logger.info(
                        "Policy [{}]: Transferred [{}] documents to enrich index [{}]",
                        policyName,
                        bulkByScrollResponse.getCreated(),
                        destinationIndexName
                    );
                    forceMergeEnrichIndex(destinationIndexName, 1);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void forceMergeEnrichIndex(final String destinationIndexName, final int attempt) {
        logger.debug(
            "Policy [{}]: Force merging newly created enrich index [{}] (Attempt {}/{})",
            policyName,
            destinationIndexName,
            attempt,
            maxForceMergeAttempts
        );
        client.admin().indices().forceMerge(new ForceMergeRequest(destinationIndexName).maxNumSegments(1), new ActionListener<>() {
            @Override
            public void onResponse(ForceMergeResponse forceMergeResponse) {
                refreshEnrichIndex(destinationIndexName, attempt);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void refreshEnrichIndex(final String destinationIndexName, final int attempt) {
        logger.debug("Policy [{}]: Refreshing enrich index [{}]", policyName, destinationIndexName);
        client.admin().indices().refresh(new RefreshRequest(destinationIndexName), new ActionListener<>() {
            @Override
            public void onResponse(RefreshResponse refreshResponse) {
                ensureSingleSegment(destinationIndexName, attempt);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    protected void ensureSingleSegment(final String destinationIndexName, final int attempt) {
        client.admin().indices().segments(new IndicesSegmentsRequest(destinationIndexName), new ActionListener<>() {
            @Override
            public void onResponse(IndicesSegmentResponse indicesSegmentResponse) {
                IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(destinationIndexName);
                if (indexSegments == null) {
                    throw new ElasticsearchException(
                        "Could not locate segment information for newly created index [{}]",
                        destinationIndexName
                    );
                }
                Map<Integer, IndexShardSegments> indexShards = indexSegments.getShards();
                assert indexShards.size() == 1 : "Expected enrich index to contain only one shard";
                ShardSegments[] shardSegments = indexShards.get(0).getShards();
                assert shardSegments.length == 1 : "Expected enrich index to contain no replicas at this point";
                ShardSegments primarySegments = shardSegments[0];
                if (primarySegments.getSegments().size() > 1) {
                    int nextAttempt = attempt + 1;
                    if (nextAttempt > maxForceMergeAttempts) {
                        listener.onFailure(
                            new ElasticsearchException(
                                "Force merging index [{}] attempted [{}] times but did not result in one segment.",
                                destinationIndexName,
                                attempt,
                                maxForceMergeAttempts
                            )
                        );
                    } else {
                        logger.debug(
                            "Policy [{}]: Force merge result contains more than one segment [{}], retrying (attempt {}/{})",
                            policyName,
                            primarySegments.getSegments().size(),
                            nextAttempt,
                            maxForceMergeAttempts
                        );
                        forceMergeEnrichIndex(destinationIndexName, nextAttempt);
                    }
                } else {
                    // Force merge down to one segment successful
                    setIndexReadOnly(destinationIndexName);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void setIndexReadOnly(final String destinationIndexName) {
        logger.debug("Policy [{}]: Setting new enrich index [{}] to be read only", policyName, destinationIndexName);
        UpdateSettingsRequest request = new UpdateSettingsRequest(destinationIndexName).setPreserveExisting(true)
            .settings(Settings.builder().put("index.auto_expand_replicas", "0-all").put("index.blocks.write", "true"));
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
        String enrichIndexBase = EnrichPolicy.getBaseName(policyName);
        logger.debug("Policy [{}]: Promoting new enrich index [{}] to alias [{}]", policyName, destinationIndexName, enrichIndexBase);
        GetAliasesRequest aliasRequest = new GetAliasesRequest(enrichIndexBase);
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, aliasRequest);
        ImmutableOpenMap<String, List<AliasMetaData>> aliases = clusterState.metaData().findAliases(aliasRequest, concreteIndices);
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
                ExecuteEnrichPolicyStatus completeStatus = new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.COMPLETE);
                task.setStatus(completeStatus);
                listener.onResponse(completeStatus);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
