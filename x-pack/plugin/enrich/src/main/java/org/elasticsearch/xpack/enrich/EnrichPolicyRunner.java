/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.enrich.action.EnrichReindexAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;

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
    private final String enrichIndexName;
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
        String enrichIndexName,
        int fetchSize,
        int maxForceMergeAttempts
    ) {
        this.policyName = Objects.requireNonNull(policyName);
        this.policy = Objects.requireNonNull(policy);
        this.task = Objects.requireNonNull(task);
        this.listener = Objects.requireNonNull(listener);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = wrapClient(client, policyName, task, clusterService);
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver);
        this.enrichIndexName = enrichIndexName;
        this.fetchSize = fetchSize;
        this.maxForceMergeAttempts = maxForceMergeAttempts;
    }

    @Override
    public void run() {
        try {
            logger.info("Policy [{}]: Running enrich policy", policyName);
            task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.RUNNING));
            // Collect the source index information
            final String[] sourceIndices = policy.getIndices().toArray(new String[0]);
            logger.debug("Policy [{}]: Checking source indices [{}]", policyName, sourceIndices);
            GetIndexRequest getIndexRequest = new GetIndexRequest().indices(sourceIndices);
            // This call does not set the origin to ensure that the user executing the policy has permission to access the source index
            client.admin().indices().getIndex(getIndexRequest, listener.delegateFailure((l, getIndexResponse) -> {
                try {
                    validateMappings(getIndexResponse);
                    prepareAndCreateEnrichIndex(toMappings(getIndexResponse));
                } catch (Exception e) {
                    l.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private List<Map<String, Object>> toMappings(GetIndexResponse response) {
        return response.mappings()
            .values()
            .stream()
            .flatMap(k -> k.values().stream())
            .map(MappingMetadata::getSourceAsMap)
            .collect(Collectors.toList());
    }

    private Map<String, Object> getMappings(final GetIndexResponse getIndexResponse, final String sourceIndexName) {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappings = getIndexResponse.mappings();
        ImmutableOpenMap<String, MappingMetadata> indexMapping = mappings.get(sourceIndexName);
        if (indexMapping.keys().size() == 0) {
            throw new ElasticsearchException(
                "Enrich policy execution for [{}] failed. No mapping available on source [{}] included in [{}]",
                policyName,
                sourceIndexName,
                policy.getIndices()
            );
        }
        assert indexMapping.keys().size() == 1 : "Expecting only one type per index";
        MappingMetadata typeMapping = indexMapping.iterator().next().value;
        return typeMapping.sourceAsMap();
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

    private XContentBuilder resolveEnrichMapping(final EnrichPolicy enrichPolicy, final List<Map<String, Object>> mappings) {
        if (EnrichPolicy.MATCH_TYPE.equals(enrichPolicy.getType())) {
            return createEnrichMappingBuilder((builder) -> builder.field("type", "keyword").field("doc_values", false));
        } else if (EnrichPolicy.RANGE_TYPE.equals(enrichPolicy.getType())) {
            return createRangeEnrichMappingBuilder(enrichPolicy, mappings);
        } else if (EnrichPolicy.GEO_MATCH_TYPE.equals(enrichPolicy.getType())) {
            return createEnrichMappingBuilder((builder) -> builder.field("type", "geo_shape"));
        } else {
            throw new ElasticsearchException("Unrecognized enrich policy type [{}]", enrichPolicy.getType());
        }
    }

    private XContentBuilder createRangeEnrichMappingBuilder(EnrichPolicy enrichPolicy, List<Map<String, Object>> mappings) {
        String matchFieldPath = "properties." + enrichPolicy.getMatchField().replace(".", ".properties.");
        List<Map<String, String>> matchFieldMappings = mappings.stream()
            .map(map -> ObjectPath.<Map<String, String>>eval(matchFieldPath, map))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        Set<String> types = matchFieldMappings.stream().map(map -> map.get("type")).collect(Collectors.toSet());
        if (types.size() == 1) {
            String type = types.iterator().next();
            switch (type) {
                case "integer_range":
                case "float_range":
                case "long_range":
                case "double_range":
                case "ip_range":
                    return createEnrichMappingBuilder((builder) -> builder.field("type", type).field("doc_values", false));

                // date_range types mappings allow for the format to be specified, should be preserved in the created index
                case "date_range":
                    Set<String> formatEntries = matchFieldMappings.stream().map(map -> map.get("format")).collect(Collectors.toSet());
                    if (formatEntries.size() == 1) {
                        return createEnrichMappingBuilder((builder) -> {
                            builder.field("type", type).field("doc_values", false);
                            String format = formatEntries.iterator().next();
                            if (format != null) {
                                builder.field("format", format);
                            }
                            return builder;
                        });
                    }
                    if (formatEntries.isEmpty()) {
                        // no format specify rely on default
                        return createEnrichMappingBuilder((builder) -> builder.field("type", type).field("doc_values", false));
                    }
                    throw new ElasticsearchException(
                        "Multiple distinct date format specified for match field '{}' - indices({})  format entries({})",
                        enrichPolicy.getMatchField(),
                        Strings.collectionToCommaDelimitedString(enrichPolicy.getIndices()),
                        (formatEntries.contains(null) ? "(DEFAULT), " : "") + Strings.collectionToCommaDelimitedString(formatEntries)
                    );

                default:
                    throw new ElasticsearchException(
                        "Field '{}' has type [{}] which doesn't appear to be a range type",
                        enrichPolicy.getMatchField(),
                        type
                    );
            }
        }
        if (types.isEmpty()) {
            throw new ElasticsearchException(
                "No mapping type found for match field '{}' - indices({})",
                enrichPolicy.getMatchField(),
                Strings.collectionToCommaDelimitedString(enrichPolicy.getIndices())
            );
        }
        throw new ElasticsearchException(
            "Multiple distinct mapping types for match field '{}' - indices({})  types({})",
            enrichPolicy.getMatchField(),
            Strings.collectionToCommaDelimitedString(enrichPolicy.getIndices()),
            Strings.collectionToCommaDelimitedString(types)
        );
    }

    private XContentBuilder createEnrichMappingBuilder(CheckedFunction<XContentBuilder, XContentBuilder, IOException> matchFieldMapping) {
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

    private void prepareAndCreateEnrichIndex(List<Map<String, Object>> mappings) {
        Settings enrichIndexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            // No changes will be made to an enrich index after policy execution, so need to enable automatic refresh interval:
            .put("index.refresh_interval", -1)
            // This disables eager global ordinals loading for all fields:
            .put("index.warmer.enabled", false)
            .build();
        CreateIndexRequest createEnrichIndexRequest = new CreateIndexRequest(enrichIndexName, enrichIndexSettings);
        createEnrichIndexRequest.mapping(MapperService.SINGLE_MAPPING_NAME, resolveEnrichMapping(policy, mappings));
        logger.debug("Policy [{}]: Creating new enrich index [{}]", policyName, enrichIndexName);
        enrichOriginClient().admin()
            .indices()
            .create(
                createEnrichIndexRequest,
                listener.delegateFailure((l, createIndexResponse) -> prepareReindexOperation(enrichIndexName))
            );
    }

    private void prepareReindexOperation(final String destinationIndexName) {
        // Check to make sure that the enrich pipeline exists, and create it if it is missing.
        if (EnrichPolicyReindexPipeline.exists(clusterService.state()) == false) {
            EnrichPolicyReindexPipeline.create(
                enrichOriginClient(),
                listener.delegateFailure((l, r) -> transferDataToEnrichIndex(destinationIndexName))
            );
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

        client.execute(
            EnrichReindexAction.INSTANCE,
            reindexRequest,
            new ActionListener.Delegating<BulkByScrollResponse, ExecuteEnrichPolicyStatus>(listener) {
                @Override
                public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                    // Do we want to fail the request if there were failures during the reindex process?
                    if (bulkByScrollResponse.getBulkFailures().size() > 0) {
                        logger.warn(
                            "Policy [{}]: encountered [{}] bulk failures. Turn on DEBUG logging for details.",
                            policyName,
                            bulkByScrollResponse.getBulkFailures().size()
                        );
                        if (logger.isDebugEnabled()) {
                            for (BulkItemResponse.Failure failure : bulkByScrollResponse.getBulkFailures()) {
                                logger.debug(
                                    new ParameterizedMessage(
                                        "Policy [{}]: bulk index failed for index [{}], id [{}]",
                                        policyName,
                                        failure.getIndex(),
                                        failure.getId()
                                    ),
                                    failure.getCause()
                                );
                            }
                        }
                        delegate.onFailure(new ElasticsearchException("Encountered bulk failures during reindex process"));
                    } else if (bulkByScrollResponse.getSearchFailures().size() > 0) {
                        logger.warn(
                            "Policy [{}]: encountered [{}] search failures. Turn on DEBUG logging for details.",
                            policyName,
                            bulkByScrollResponse.getSearchFailures().size()
                        );
                        if (logger.isDebugEnabled()) {
                            for (ScrollableHitSource.SearchFailure failure : bulkByScrollResponse.getSearchFailures()) {
                                logger.debug(
                                    new ParameterizedMessage(
                                        "Policy [{}]: search failed for index [{}], shard [{}] on node [{}]",
                                        policyName,
                                        failure.getIndex(),
                                        failure.getShardId(),
                                        failure.getNodeId()
                                    ),
                                    failure.getReason()
                                );
                            }
                        }
                        delegate.onFailure(new ElasticsearchException("Encountered search failures during reindex process"));
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
            }
        );
    }

    private void forceMergeEnrichIndex(final String destinationIndexName, final int attempt) {
        logger.debug(
            "Policy [{}]: Force merging newly created enrich index [{}] (Attempt {}/{})",
            policyName,
            destinationIndexName,
            attempt,
            maxForceMergeAttempts
        );
        enrichOriginClient().admin()
            .indices()
            .forceMerge(
                new ForceMergeRequest(destinationIndexName).maxNumSegments(1),
                listener.delegateFailure((l, r) -> refreshEnrichIndex(destinationIndexName, attempt))
            );
    }

    private void refreshEnrichIndex(final String destinationIndexName, final int attempt) {
        logger.debug("Policy [{}]: Refreshing enrich index [{}]", policyName, destinationIndexName);
        enrichOriginClient().admin()
            .indices()
            .refresh(
                new RefreshRequest(destinationIndexName),
                listener.delegateFailure((l, r) -> ensureSingleSegment(destinationIndexName, attempt))
            );
    }

    protected void ensureSingleSegment(final String destinationIndexName, final int attempt) {
        enrichOriginClient().admin()
            .indices()
            .segments(
                new IndicesSegmentsRequest(destinationIndexName),
                new ActionListener.Delegating<IndicesSegmentResponse, ExecuteEnrichPolicyStatus>(listener) {
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
                                delegate.onFailure(
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
                }
            );
    }

    private void setIndexReadOnly(final String destinationIndexName) {
        logger.debug("Policy [{}]: Setting new enrich index [{}] to be read only", policyName, destinationIndexName);
        UpdateSettingsRequest request = new UpdateSettingsRequest(destinationIndexName).setPreserveExisting(true)
            .settings(Settings.builder().put("index.auto_expand_replicas", "0-all").put("index.blocks.write", "true"));
        enrichOriginClient().admin()
            .indices()
            .updateSettings(request, listener.delegateFailure((l, r) -> waitForIndexGreen(destinationIndexName)));
    }

    private void waitForIndexGreen(final String destinationIndexName) {
        ClusterHealthRequest request = new ClusterHealthRequest(destinationIndexName).waitForGreenStatus();
        enrichOriginClient().admin()
            .cluster()
            .health(request, listener.delegateFailure((l, r) -> updateEnrichPolicyAlias(destinationIndexName)));
    }

    private void updateEnrichPolicyAlias(final String destinationIndexName) {
        String enrichIndexBase = EnrichPolicy.getBaseName(policyName);
        logger.debug("Policy [{}]: Promoting new enrich index [{}] to alias [{}]", policyName, destinationIndexName, enrichIndexBase);
        GetAliasesRequest aliasRequest = new GetAliasesRequest(enrichIndexBase);
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(clusterState, aliasRequest);
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = clusterState.metadata().findAliases(aliasRequest, concreteIndices);
        IndicesAliasesRequest aliasToggleRequest = new IndicesAliasesRequest();
        String[] indices = aliases.keys().toArray(String.class);
        if (indices.length > 0) {
            aliasToggleRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().indices(indices).alias(enrichIndexBase));
        }
        aliasToggleRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(destinationIndexName).alias(enrichIndexBase));
        enrichOriginClient().admin().indices().aliases(aliasToggleRequest, listener.delegateFailure((l, r) -> {
            logger.info("Policy [{}]: Policy execution complete", policyName);
            ExecuteEnrichPolicyStatus completeStatus = new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.COMPLETE);
            task.setStatus(completeStatus);
            l.onResponse(completeStatus);
        }));
    }

    /**
     * Use this client to access information at the access level of the Enrich plugin, rather than at the access level of the user.
     * For example, use this client to access system indices (such as `.enrich*` indices).
     */
    private Client enrichOriginClient() {
        return new OriginSettingClient(client, ENRICH_ORIGIN);
    }

    private static Client wrapClient(Client in, String policyName, ExecuteEnrichPolicyTask task, ClusterService clusterService) {
        // Filter client in order to:
        // 1) Check on transport action call that policy runner does whether the task has been cancelled
        // 2) Set the enrich policy task as parent task, so if other API calls (e.g. reindex) are cancellable then
        // the corresponding tasks of these API calls get cancelled as well.
        return new FilterClient(in) {

            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                String requestStep = request.getClass().getSimpleName();
                task.setStep(requestStep);
                if (task.isCancelled()) {
                    String message = "cancelled policy execution [" + policyName + "], status [" + Strings.toString(task.getStatus()) + "]";
                    listener.onFailure(new TaskCancelledException(message));
                    return;
                }
                request.setParentTask(clusterService.localNode().getId(), task.getId());
                super.doExecute(action, request, listener);
            }
        };
    }
}
