/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
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
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.enrich.action.EnrichReindexAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;

public class EnrichPolicyRunner {

    private static final Logger logger = LogManager.getLogger(EnrichPolicyRunner.class);

    static final String ENRICH_POLICY_NAME_FIELD_NAME = "enrich_policy_name";
    static final String ENRICH_POLICY_TYPE_FIELD_NAME = "enrich_policy_type";
    static final String ENRICH_MATCH_FIELD_NAME = "enrich_match_field";
    static final String ENRICH_README_FIELD_NAME = "enrich_readme";

    public static final String ENRICH_MIN_NUMBER_OF_REPLICAS_NAME = "enrich.min_number_of_replicas";

    static final String ENRICH_INDEX_README_TEXT = "This index is managed by Elasticsearch and should not be modified in any way.";

    /**
     * Timeout for enrich-related requests that interact with the master node. Possibly this should be longer and/or configurable.
     */
    static final TimeValue ENRICH_MASTER_REQUEST_TIMEOUT = TimeValue.THIRTY_SECONDS;

    private final ProjectId projectId;
    private final String policyName;
    private final EnrichPolicy policy;
    private final ExecuteEnrichPolicyTask task;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final Client client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final String enrichIndexName;
    private final int fetchSize;
    private final int maxForceMergeAttempts;

    EnrichPolicyRunner(
        ProjectId projectId,
        String policyName,
        EnrichPolicy policy,
        ExecuteEnrichPolicyTask task,
        ClusterService clusterService,
        IndicesService indicesService,
        Client client,
        IndexNameExpressionResolver indexNameExpressionResolver,
        String enrichIndexName,
        int fetchSize,
        int maxForceMergeAttempts
    ) {
        this.projectId = projectId;
        this.policyName = Objects.requireNonNull(policyName);
        this.policy = Objects.requireNonNull(policy);
        this.task = Objects.requireNonNull(task);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.indicesService = indicesService;
        this.client = wrapClient(client, policyName, task, clusterService);
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver);
        this.enrichIndexName = enrichIndexName;
        this.fetchSize = fetchSize;
        this.maxForceMergeAttempts = maxForceMergeAttempts;
    }

    public void run(ActionListener<ExecuteEnrichPolicyStatus> listener) {
        logger.info("Policy [{}]: Running enrich policy", policyName);
        task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.RUNNING));

        SubscribableListener

            .<GetIndexResponse>newForked(l -> {
                // Collect the source index information
                final String[] sourceIndices = policy.getIndices().toArray(new String[0]);
                logger.debug("Policy [{}]: Checking source indices [{}]", policyName, sourceIndices);
                GetIndexRequest getIndexRequest = new GetIndexRequest(ENRICH_MASTER_REQUEST_TIMEOUT).indices(sourceIndices);
                // This call does not set the origin to ensure that the user executing the policy has permission to access the source index
                client.admin().indices().getIndex(getIndexRequest, l);
            })
            .<CreateIndexResponse>andThen((l, getIndexResponse) -> {
                validateMappings(getIndexResponse);
                prepareAndCreateEnrichIndex(toMappings(getIndexResponse), clusterService.getSettings(), l);
            })
            .andThen(this::prepareReindexOperation)
            .andThen(this::transferDataToEnrichIndex)
            .andThen(this::forceMergeEnrichIndex)
            .andThen(this::setIndexReadOnly)
            .andThen(this::waitForIndexGreen)
            .andThen(this::updateEnrichPolicyAlias)
            .andThenApply(r -> {
                logger.info("Policy [{}]: Policy execution complete", policyName);
                ExecuteEnrichPolicyStatus completeStatus = new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.COMPLETE);
                task.setStatus(completeStatus);
                return completeStatus;
            })
            .addListener(listener);
    }

    private static List<Map<String, Object>> toMappings(GetIndexResponse response) {
        return response.mappings().values().stream().map(MappingMetadata::getSourceAsMap).collect(Collectors.toList());
    }

    private Map<String, Object> getMappings(final GetIndexResponse getIndexResponse, final String sourceIndexName) {
        Map<String, MappingMetadata> mappings = getIndexResponse.mappings();
        MappingMetadata indexMapping = mappings.get(sourceIndexName);
        if (MappingMetadata.EMPTY_MAPPINGS.equals(indexMapping)) {
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
            validateAndGetMappingTypeAndFormat(mapping, policy.getMatchField(), true);
            for (String valueFieldName : policy.getEnrichFields()) {
                validateAndGetMappingTypeAndFormat(mapping, valueFieldName, false);
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

    private record MappingTypeAndFormat(String type, String format) {}

    private static MappingTypeAndFormat validateAndGetMappingTypeAndFormat(
        String fieldName,
        EnrichPolicy policy,
        boolean strictlyRequired,
        List<Map<String, Object>> sourceMappings
    ) {
        var fieldMappings = sourceMappings.stream()
            .map(mapping -> validateAndGetMappingTypeAndFormat(mapping, fieldName, strictlyRequired))
            .filter(Objects::nonNull)
            .toList();
        Set<String> types = fieldMappings.stream().map(tf -> tf.type).collect(Collectors.toSet());
        if (types.size() > 1) {
            if (strictlyRequired) {
                throw new ElasticsearchException(
                    "Multiple distinct mapping types for field '{}' - indices({})  types({})",
                    fieldName,
                    Strings.collectionToCommaDelimitedString(policy.getIndices()),
                    Strings.collectionToCommaDelimitedString(types)
                );
            }
            return null;
        }
        if (types.isEmpty()) {
            return null;
        }
        Set<String> formats = fieldMappings.stream().map(tf -> tf.format).filter(Objects::nonNull).collect(Collectors.toSet());
        if (formats.size() > 1) {
            if (strictlyRequired) {
                throw new ElasticsearchException(
                    "Multiple distinct formats specified for field '{}' - indices({})  format entries({})",
                    policy.getMatchField(),
                    Strings.collectionToCommaDelimitedString(policy.getIndices()),
                    Strings.collectionToCommaDelimitedString(formats)
                );
            }
            return null;
        }
        return new MappingTypeAndFormat(Iterables.get(types, 0), formats.isEmpty() ? null : Iterables.get(formats, 0));
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValues(Map<String, Object> properties, String path) {
        return (T) properties.get(path);
    }

    private static MappingTypeAndFormat validateAndGetMappingTypeAndFormat(
        Map<String, Object> properties,
        String fieldName,
        boolean fieldRequired
    ) {
        assert Strings.isEmpty(fieldName) == false : "Field name cannot be null or empty";
        String[] fieldParts = fieldName.split("\\.");
        StringBuilder parent = new StringBuilder();
        Map<String, Object> currentField = properties;
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
            Map<String, Object> currentProperties = extractValues(currentField, "properties");
            if (currentProperties == null) {
                if (fieldRequired) {
                    throw new ElasticsearchException(
                        "Could not traverse mapping to field [{}]. Expected the [{}] field to have sub fields but none were configured.",
                        fieldName,
                        onRoot ? "root" : parent.toString()
                    );
                } else {
                    return null;
                }
            }
            currentField = extractValues(currentProperties, fieldPart);
            if (currentField == null) {
                if (fieldRequired) {
                    throw new ElasticsearchException(
                        "Could not traverse mapping to field [{}]. Could not find the [{}] field under [{}]",
                        fieldName,
                        fieldPart,
                        onRoot ? "root" : parent.toString()
                    );
                } else {
                    return null;
                }
            }
            if (onRoot) {
                onRoot = false;
            } else {
                parent.append(".");
            }
            parent.append(fieldPart);
        }
        if (currentField == null) {
            return null;
        }
        final String type = (String) currentField.getOrDefault("type", "object");
        final String format = (String) currentField.get("format");
        return new MappingTypeAndFormat(type, format);
    }

    static final Set<String> RANGE_TYPES = Set.of("integer_range", "float_range", "long_range", "double_range", "ip_range", "date_range");

    static Map<String, Object> mappingForMatchField(EnrichPolicy policy, List<Map<String, Object>> sourceMappings) {
        MappingTypeAndFormat typeAndFormat = validateAndGetMappingTypeAndFormat(policy.getMatchField(), policy, true, sourceMappings);
        if (typeAndFormat == null) {
            throw new ElasticsearchException(
                "Match field '{}' doesn't have a correct mapping type for policy type '{}'",
                policy.getMatchField(),
                policy.getType()
            );
        }
        return switch (policy.getType()) {
            case EnrichPolicy.MATCH_TYPE -> Map.of("type", "keyword", "doc_values", false);
            case EnrichPolicy.GEO_MATCH_TYPE -> Map.of("type", "geo_shape");
            case EnrichPolicy.RANGE_TYPE -> {
                if (RANGE_TYPES.contains(typeAndFormat.type) == false) {
                    throw new ElasticsearchException(
                        "Field '{}' has type [{}] which doesn't appear to be a range type",
                        policy.getMatchField(),
                        typeAndFormat.type
                    );
                }
                Map<String, Object> mapping = Maps.newMapWithExpectedSize(3);
                mapping.put("type", typeAndFormat.type);
                mapping.put("doc_values", false);
                if (typeAndFormat.format != null) {
                    mapping.put("format", typeAndFormat.format);
                }
                yield mapping;
            }
            default -> throw new ElasticsearchException("Unrecognized enrich policy type [{}]", policy.getType());
        };
    }

    private XContentBuilder createEnrichMapping(List<Map<String, Object>> sourceMappings) {
        Map<String, Map<String, Object>> fieldMappings = new HashMap<>();
        Map<String, Object> mappingForMatchField = mappingForMatchField(policy, sourceMappings);
        MapperService mapperService = createMapperServiceForValidation(indicesService, enrichIndexName);
        for (String enrichField : policy.getEnrichFields()) {
            if (enrichField.equals(policy.getMatchField())) {
                mappingForMatchField = new HashMap<>(mappingForMatchField);
                mappingForMatchField.remove("doc_values"); // enable doc_values
            } else {
                var typeAndFormat = validateAndGetMappingTypeAndFormat(enrichField, policy, false, sourceMappings);
                if (typeAndFormat != null) {
                    Map<String, Object> mapping = Maps.newMapWithExpectedSize(3);
                    mapping.put("type", typeAndFormat.type);
                    if (typeAndFormat.format != null) {
                        mapping.put("format", typeAndFormat.format);
                    }
                    if (isIndexableField(mapperService, enrichField, typeAndFormat.type, mapping)) {
                        mapping.put("index", false);
                    }
                    fieldMappings.put(enrichField, mapping);
                }
            }
        }
        fieldMappings.put(policy.getMatchField(), mappingForMatchField);

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
                        builder.mapContents(fieldMappings);
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

    private static MapperService createMapperServiceForValidation(IndicesService indicesService, String index) {
        try {
            final Settings idxSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();
            IndexMetadata indexMetadata = IndexMetadata.builder(index).settings(idxSettings).numberOfShards(1).numberOfReplicas(0).build();
            return indicesService.createIndexMapperServiceForValidation(indexMetadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static boolean isIndexableField(MapperService mapperService, String field, String type, Map<String, Object> properties) {
        var withIndexParameter = new HashMap<>(properties);
        withIndexParameter.put("index", false);
        Mapper.TypeParser parser = mapperService.getMapperRegistry().getMapperParser(type, IndexVersion.current());
        try {
            parser.parse(field, withIndexParameter, mapperService.parserContext());
            return withIndexParameter.containsKey("index") == false;
        } catch (MapperParsingException e) {
            // hitting the mapper parsing exception means this field doesn't accept `index:false`.
            assert e.getMessage().contains("unknown parameter [index]") : e;
            return false;
        }
    }

    private void prepareAndCreateEnrichIndex(
        List<Map<String, Object>> mappings,
        Settings settings,
        ActionListener<CreateIndexResponse> listener
    ) {
        int numberOfReplicas = settings.getAsInt(ENRICH_MIN_NUMBER_OF_REPLICAS_NAME, 0);
        Settings enrichIndexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", numberOfReplicas)
            // No changes will be made to an enrich index after policy execution, so need to enable automatic refresh interval:
            .put("index.refresh_interval", -1)
            // This disables eager global ordinals loading for all fields:
            .put("index.warmer.enabled", false)
            .build();
        CreateIndexRequest createEnrichIndexRequest = new CreateIndexRequest(enrichIndexName, enrichIndexSettings);
        createEnrichIndexRequest.mapping(createEnrichMapping(mappings));
        logger.debug("Policy [{}]: Creating new enrich index [{}]", policyName, enrichIndexName);
        enrichOriginClient().admin().indices().create(createEnrichIndexRequest, listener);
    }

    private void prepareReindexOperation(ActionListener<AcknowledgedResponse> listener) {
        // Check to make sure that the enrich pipeline exists, and create it if it is missing.
        if (EnrichPolicyReindexPipeline.exists(clusterService.state().getMetadata().getProject(projectId))) {
            listener.onResponse(null);
        } else {
            EnrichPolicyReindexPipeline.create(enrichOriginClient(), listener);
        }
    }

    private void transferDataToEnrichIndex(ActionListener<Void> listener) {
        logger.debug("Policy [{}]: Transferring source data to new enrich index [{}]", policyName, enrichIndexName);
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
        ReindexRequest reindexRequest = new ReindexRequest().setDestIndex(enrichIndexName)
            .setSourceIndices(policy.getIndices().toArray(new String[0]));
        reindexRequest.getSearchRequest().source(searchSourceBuilder);
        reindexRequest.getDestination().source(new BytesArray(new byte[0]), XContentType.SMILE);
        reindexRequest.getDestination().routing("discard");
        reindexRequest.getDestination().setPipeline(EnrichPolicyReindexPipeline.pipelineName());

        client.execute(EnrichReindexAction.INSTANCE, reindexRequest, new DelegatingActionListener<>(listener) {
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
                                () -> format(
                                    "Policy [%s]: bulk index failed for index [%s], id [%s]",
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
                                () -> format(
                                    "Policy [%s]: search failed for index [%s], shard [%s] on node [%s]",
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
                        enrichIndexName
                    );
                    delegate.onResponse(null);
                }
            }
        });
    }

    private void forceMergeEnrichIndex(ActionListener<Void> listener) {
        forceMergeEnrichIndexOrRetry(1, listener);
    }

    private void forceMergeEnrichIndexOrRetry(final int attempt, ActionListener<Void> listener) {
        logger.debug(
            "Policy [{}]: Force merging newly created enrich index [{}] (Attempt {}/{})",
            policyName,
            enrichIndexName,
            attempt,
            maxForceMergeAttempts
        );

        SubscribableListener

            .<BroadcastResponse>newForked(
                l -> enrichOriginClient().admin().indices().forceMerge(new ForceMergeRequest(enrichIndexName).maxNumSegments(1), l)
            )
            .andThen(this::refreshEnrichIndex)
            .andThen(this::afterRefreshEnrichIndex)
            .andThen(this::getSegments)
            .andThenApply(this::getSegmentCount)
            .addListener(
                // delegateFailureAndWrap() rather than andThen().addListener() to avoid building unnecessary O(#retries) listener chain
                listener.delegateFailureAndWrap((l, segmentCount) -> {
                    if (segmentCount > 1) {
                        int nextAttempt = attempt + 1;
                        if (nextAttempt > maxForceMergeAttempts) {
                            throw new ElasticsearchException(
                                "Force merging index [{}] attempted [{}] times but did not result in one segment.",
                                enrichIndexName,
                                attempt,
                                maxForceMergeAttempts
                            );
                        } else {
                            logger.debug(
                                "Policy [{}]: Force merge result contains more than one segment [{}], retrying (attempt {}/{})",
                                policyName,
                                segmentCount,
                                nextAttempt,
                                maxForceMergeAttempts
                            );
                            // TransportForceMergeAction always forks so no risk of stack overflow from this recursion
                            forceMergeEnrichIndexOrRetry(nextAttempt, l);
                        }
                    } else {
                        l.onResponse(null);
                    }
                })
            );
    }

    private void refreshEnrichIndex(ActionListener<BroadcastResponse> listener) {
        logger.debug("Policy [{}]: Refreshing enrich index [{}]", policyName, enrichIndexName);
        enrichOriginClient().admin().indices().refresh(new RefreshRequest(enrichIndexName), listener);
    }

    // hook to allow testing force-merge retries
    protected void afterRefreshEnrichIndex(ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    private void getSegments(ActionListener<IndicesSegmentResponse> listener) {
        enrichOriginClient().admin().indices().segments(new IndicesSegmentsRequest(enrichIndexName), listener);
    }

    private int getSegmentCount(IndicesSegmentResponse indicesSegmentResponse) {
        int failedShards = indicesSegmentResponse.getFailedShards();
        if (failedShards > 0) {
            // Encountered a problem while querying the segments for the enrich index. Try and surface the problem in the log.
            logger.warn(
                "Policy [{}]: Encountered [{}] shard level failures while querying the segments for enrich index [{}]. "
                    + "Turn on DEBUG logging for details.",
                policyName,
                failedShards,
                enrichIndexName
            );
            if (logger.isDebugEnabled()) {
                DefaultShardOperationFailedException[] shardFailures = indicesSegmentResponse.getShardFailures();
                int failureNumber = 1;
                String logPrefix = "Policy [" + policyName + "]: Encountered shard failure [";
                String logSuffix = " of "
                    + shardFailures.length
                    + "] while querying segments for enrich index ["
                    + enrichIndexName
                    + "]. Shard [";
                for (DefaultShardOperationFailedException shardFailure : shardFailures) {
                    logger.debug(
                        logPrefix + failureNumber + logSuffix + shardFailure.index() + "][" + shardFailure.shardId() + "]",
                        shardFailure.getCause()
                    );
                    failureNumber++;
                }
            }
        }
        IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(enrichIndexName);
        if (indexSegments == null) {
            if (indicesSegmentResponse.getShardFailures().length == 0) {
                throw new ElasticsearchException("Could not locate segment information for newly created index [{}]", enrichIndexName);
            } else {
                DefaultShardOperationFailedException shardFailure = indicesSegmentResponse.getShardFailures()[0];
                throw new ElasticsearchException(
                    "Could not obtain segment information for newly created index [{}]; shard info [{}][{}]",
                    shardFailure.getCause(),
                    enrichIndexName,
                    shardFailure.index(),
                    shardFailure.shardId()
                );
            }
        }
        Map<Integer, IndexShardSegments> indexShards = indexSegments.getShards();
        assert indexShards.size() == 1 : "Expected enrich index to contain only one shard";
        ShardSegments[] shardSegments = indexShards.get(0).shards();
        assert shardSegments.length == 1 : "Expected enrich index to contain no replicas at this point";
        ShardSegments primarySegments = shardSegments[0];
        return primarySegments.getSegments().size();
    }

    private void setIndexReadOnly(ActionListener<AcknowledgedResponse> listener) {
        logger.debug("Policy [{}]: Setting new enrich index [{}] to be read only", policyName, enrichIndexName);
        UpdateSettingsRequest request = new UpdateSettingsRequest(enrichIndexName).setPreserveExisting(true)
            .settings(Settings.builder().put("index.auto_expand_replicas", "0-all").put("index.blocks.write", "true"));
        enrichOriginClient().admin().indices().updateSettings(request, listener);
    }

    private void waitForIndexGreen(ActionListener<ClusterHealthResponse> listener) {
        ClusterHealthRequest request = new ClusterHealthRequest(ENRICH_MASTER_REQUEST_TIMEOUT, enrichIndexName).waitForGreenStatus();
        enrichOriginClient().admin().cluster().health(request, listener);
    }

    /**
     * Ensures that the index we are about to promote at the end of a policy execution exists, is intact, and has not been damaged
     * during the policy execution. In some cases, it is possible for the index being constructed to be deleted during the policy execution
     * and recreated with invalid mappings/data. We validate that the mapping exists and that it contains the expected meta fields on it to
     * guard against accidental removal and recreation during policy execution.
     */
    private void validateIndexBeforePromotion(String destinationIndexName, ProjectMetadata project) {
        IndexMetadata destinationIndex = project.index(destinationIndexName);
        if (destinationIndex == null) {
            throw new IndexNotFoundException(
                "was not able to promote it as part of executing enrich policy [" + policyName + "]",
                destinationIndexName
            );
        }
        MappingMetadata mapping = destinationIndex.mapping();
        if (mapping == null) {
            throw new ResourceNotFoundException(
                "Could not locate mapping for enrich index [{}] while completing [{}] policy run",
                destinationIndexName,
                policyName
            );
        }
        Map<String, Object> mappingSource = mapping.sourceAsMap();
        Object meta = mappingSource.get("_meta");
        if (meta instanceof Map<?, ?> metaMap) {
            Object policyNameMetaField = metaMap.get(ENRICH_POLICY_NAME_FIELD_NAME);
            if (policyNameMetaField == null) {
                throw new ElasticsearchException(
                    "Could not verify enrich index [{}] metadata before completing [{}] policy run: policy name meta field missing",
                    destinationIndexName,
                    policyName
                );
            } else if (policyName.equals(policyNameMetaField) == false) {
                throw new ElasticsearchException(
                    "Could not verify enrich index [{}] metadata before completing [{}] policy run: policy name meta field does not "
                        + "match expected value of [{}], was [{}]",
                    destinationIndexName,
                    policyName,
                    policyName,
                    policyNameMetaField.toString()
                );
            }
        } else {
            throw new ElasticsearchException(
                "Could not verify enrich index [{}] metadata before completing [{}] policy run: mapping meta field missing",
                destinationIndexName,
                policyName
            );
        }
    }

    private void updateEnrichPolicyAlias(ActionListener<IndicesAliasesResponse> listener) {
        String enrichIndexBase = EnrichPolicy.getBaseName(policyName);
        logger.debug("Policy [{}]: Promoting new enrich index [{}] to alias [{}]", policyName, enrichIndexName, enrichIndexBase);
        GetAliasesRequest aliasRequest = new GetAliasesRequest(ENRICH_MASTER_REQUEST_TIMEOUT, enrichIndexBase);
        final var project = clusterService.state().metadata().getProject(projectId);
        validateIndexBeforePromotion(enrichIndexName, project);
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(project, aliasRequest);
        String[] aliases = aliasRequest.aliases();
        IndicesAliasesRequest aliasToggleRequest = new IndicesAliasesRequest(ENRICH_MASTER_REQUEST_TIMEOUT, ENRICH_MASTER_REQUEST_TIMEOUT);
        String[] indices = project.findAliases(aliases, concreteIndices).keySet().toArray(new String[0]);
        if (indices.length > 0) {
            aliasToggleRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().indices(indices).alias(enrichIndexBase));
        }
        aliasToggleRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(enrichIndexName).alias(enrichIndexBase));
        enrichOriginClient().admin().indices().aliases(aliasToggleRequest, listener);
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
