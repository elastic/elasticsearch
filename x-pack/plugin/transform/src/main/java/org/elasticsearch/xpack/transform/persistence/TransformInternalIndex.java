/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.transform.TransformField.TRANSFORM_ID;

public final class TransformInternalIndex {

    /* Changelog of internal index versions
     *
     * Please list changes, increase the version in @link{TransformInternalIndexConstants} if you are 1st in this release cycle
     *
     * version 1 (7.2): initial
     * version 2 (7.4): cleanup, add config::version, config::create_time, checkpoint::timestamp, checkpoint::time_upper_bound,
     *                  progress::docs_processed, progress::docs_indexed,
     *                  stats::exponential_avg_checkpoint_duration_ms, stats::exponential_avg_documents_indexed,
     *                  stats::exponential_avg_documents_processed
     * version 3 (7.5): rename to .transform-internal-xxx
     * version 4 (7.6): state::should_stop_at_checkpoint
     *                  checkpoint::checkpoint
     * version 5 (7.7): stats::processing_time_in_ms, stats::processing_total
     * version 6 (7.12):stats::delete_time_in_ms, stats::documents_deleted
     * version 7 (7.13):add mapping for config::pivot, config::latest, config::retention_policy and config::sync
     */

    // constants for mappings
    public static final String DYNAMIC = "dynamic";
    public static final String PROPERTIES = "properties";
    public static final String TYPE = "type";
    public static final String ENABLED = "enabled";
    public static final String DATE = "date";
    public static final String TEXT = "text";
    public static final String FIELDS = "fields";
    public static final String RAW = "raw";
    public static final String IGNORE_ABOVE = "ignore_above";

    // data types
    public static final String FLOAT = "float";
    public static final String DOUBLE = "double";
    public static final String LONG = "long";
    public static final String KEYWORD = "keyword";
    public static final String BOOLEAN = "boolean";
    public static final String FLATTENED = "flattened";

    public static SystemIndexDescriptor getSystemIndexDescriptor(Settings transformInternalIndexAdditionalSettings) throws IOException {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(TransformInternalIndexConstants.INDEX_NAME_PATTERN)
            .setPrimaryIndex(TransformInternalIndexConstants.LATEST_INDEX_NAME)
            .setDescription("Contains Transform configuration data")
            .setMappings(mappings())
            .setSettings(settings(transformInternalIndexAdditionalSettings))
            .setVersionMetaKey("version")
            .setOrigin(TRANSFORM_ORIGIN)
            .build();
    }

    public static Template getAuditIndexTemplate() throws IOException {
        AliasMetadata alias = AliasMetadata.builder(TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS).isHidden(true).build();

        return new Template(
            Settings.builder()
                // the audits are expected to be small
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
                .build(),
            new CompressedXContent(Strings.toString(auditMappings())),
            Collections.singletonMap(alias.alias(), alias)
        );
    }

    private static XContentBuilder auditMappings() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.startObject(SINGLE_MAPPING_NAME);
        addMetaInformation(builder);
        builder.field(DYNAMIC, "false");
        builder.startObject(PROPERTIES)
            .startObject(TRANSFORM_ID)
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(AbstractAuditMessage.LEVEL.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(AbstractAuditMessage.MESSAGE.getPreferredName())
            .field(TYPE, TEXT)
            .startObject(FIELDS)
            .startObject(RAW)
            .field(TYPE, KEYWORD)
            .field(IGNORE_ABOVE, 1024)
            .endObject()
            .endObject()
            .endObject()
            .startObject(AbstractAuditMessage.TIMESTAMP.getPreferredName())
            .field(TYPE, DATE)
            .endObject()
            .startObject(AbstractAuditMessage.NODE_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        return builder;
    }

    public static Settings settings(Settings additionalSettings) {
        assert additionalSettings != null;
        return Settings.builder()
            // the configurations are expected to be small
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(additionalSettings)
            .build();
    }

    public static XContentBuilder mappings() throws IOException {
        XContentBuilder builder = jsonBuilder();
        return mappings(builder);
    }

    public static XContentBuilder mappings(XContentBuilder builder) throws IOException {
        builder.startObject();

        builder.startObject(MapperService.SINGLE_MAPPING_NAME);
        addMetaInformation(builder);

        // do not allow anything outside of the defined schema
        builder.field(DYNAMIC, "false");
        // the schema definitions
        builder.startObject(PROPERTIES);
        // overall doc type
        builder.startObject(TransformField.INDEX_DOC_TYPE.getPreferredName()).field(TYPE, KEYWORD).endObject();
        // add the schema for transform configurations
        addTransformsConfigMappings(builder);
        // add the schema for transform stats
        addTransformStoredDocMappings(builder);
        // add the schema for checkpoints
        addTransformCheckpointMappings(builder);
        // end type
        builder.endObject();
        // end properties
        builder.endObject();
        // end mapping
        builder.endObject();
        return builder;
    }

    private static XContentBuilder addTransformStoredDocMappings(XContentBuilder builder) throws IOException {
        return builder.startObject(TransformStoredDoc.STATE_FIELD.getPreferredName())
            .startObject(PROPERTIES)
            .startObject(TransformState.TASK_STATE.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TransformState.INDEXER_STATE.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TransformState.SHOULD_STOP_AT_NEXT_CHECKPOINT.getPreferredName())
            .field(TYPE, BOOLEAN)
            .endObject()
            .startObject(TransformState.CURRENT_POSITION.getPreferredName())
            .field(ENABLED, false)
            .endObject()
            .startObject(TransformState.CHECKPOINT.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformState.REASON.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TransformState.PROGRESS.getPreferredName())
            .startObject(PROPERTIES)
            .startObject(TransformProgress.TOTAL_DOCS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformProgress.DOCS_REMAINING.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformProgress.PERCENT_COMPLETE)
            .field(TYPE, FLOAT)
            .endObject()
            .startObject(TransformProgress.DOCS_INDEXED.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformProgress.DOCS_PROCESSED.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .startObject(TransformField.STATS_FIELD.getPreferredName())
            .startObject(PROPERTIES)
            .startObject(TransformIndexerStats.NUM_PAGES.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.NUM_INPUT_DOCUMENTS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.NUM_OUTPUT_DOCUMENTS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.NUM_DELETED_DOCUMENTS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.NUM_INVOCATIONS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.INDEX_TIME_IN_MS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.SEARCH_TIME_IN_MS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.PROCESSING_TIME_IN_MS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.DELETE_TIME_IN_MS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.INDEX_TOTAL.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.SEARCH_TOTAL.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.PROCESSING_TOTAL.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.SEARCH_FAILURES.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.INDEX_FAILURES.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TransformIndexerStats.EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName())
            .field(TYPE, DOUBLE)
            .endObject()
            .startObject(TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName())
            .field(TYPE, DOUBLE)
            .endObject()
            .startObject(TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName())
            .field(TYPE, DOUBLE)
            .endObject()
            .endObject()
            .endObject();
    }

    public static XContentBuilder addTransformsConfigMappings(XContentBuilder builder) throws IOException {
        return builder.startObject(TransformField.ID.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TransformField.SOURCE.getPreferredName())
            .startObject(PROPERTIES)
            .startObject(SourceConfig.INDEX.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(SourceConfig.QUERY.getPreferredName())
            .field(ENABLED, false)
            .endObject()
            .endObject()
            .endObject()
            .startObject(TransformField.DESTINATION.getPreferredName())
            .startObject(PROPERTIES)
            .startObject(DestConfig.INDEX.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .endObject()
            .endObject()
            .startObject(TransformField.DESCRIPTION.getPreferredName())
            .field(TYPE, TEXT)
            .endObject()
            .startObject(TransformField.VERSION.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TransformField.CREATE_TIME.getPreferredName())
            .field(TYPE, DATE)
            .endObject()
            .startObject(TransformConfig.Function.PIVOT.getParseField().getPreferredName())
            .field(TYPE, FLATTENED)
            .endObject()
            .startObject(TransformConfig.Function.LATEST.getParseField().getPreferredName())
            .field(TYPE, FLATTENED)
            .endObject()
            .startObject(TransformField.METADATA.getPreferredName())
            .field(TYPE, FLATTENED)
            .endObject()
            .startObject(TransformField.RETENTION_POLICY.getPreferredName())
            .field(TYPE, FLATTENED)
            .endObject()
            .startObject(TransformField.SYNC.getPreferredName())
            .field(TYPE, FLATTENED)
            .endObject();
    }

    private static XContentBuilder addTransformCheckpointMappings(XContentBuilder builder) throws IOException {
        return builder.startObject(TransformField.TIMESTAMP_MILLIS.getPreferredName())
            .field(TYPE, DATE)
            .endObject()
            .startObject(TransformField.TIME_UPPER_BOUND_MILLIS.getPreferredName())
            .field(TYPE, DATE)
            .endObject()
            .startObject(TransformCheckpoint.CHECKPOINT.getPreferredName())
            .field(TYPE, LONG)
            .endObject();
    }

    /**
     * Inserts "_meta" containing useful information like the version into the mapping
     * template.
     *
     * @param builder The builder for the mappings
     * @throws IOException On write error
     */
    private static XContentBuilder addMetaInformation(XContentBuilder builder) throws IOException {
        return builder.startObject("_meta").field("version", Version.CURRENT).endObject();
    }

    protected static boolean hasLatestVersionedIndex(ClusterState state) {
        return state.getMetadata().hasIndexAbstraction(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME);
    }

    protected static boolean allPrimaryShardsActiveForLatestVersionedIndex(ClusterState state) {
        IndexRoutingTable indexRouting = state.routingTable().index(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME);

        return indexRouting != null && indexRouting.allPrimaryShardsActive();
    }

    private static void waitForLatestVersionedIndexShardsActive(Client client, ActionListener<Void> listener) {
        ClusterHealthRequest request = new ClusterHealthRequest(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME)
            // cluster health does not wait for active shards per default
            .waitForActiveShards(ActiveShardCount.ONE);
        ActionListener<ClusterHealthResponse> innerListener = ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            TRANSFORM_ORIGIN,
            request,
            innerListener,
            client.admin().cluster()::health
        );
    }

    /**
     * This method should be called before any document is indexed that relies on the
     * existence of the latest internal index.
     *
     * Without this check the data nodes will create an internal index with dynamic
     * mappings when indexing a document, but we want our own well defined mappings.
     */
    public static void createLatestVersionedIndexIfRequired(
        ClusterService clusterService,
        Client client,
        Settings transformInternalIndexAdditionalSettings,
        ActionListener<Void> listener
    ) {
        ClusterState state = clusterService.state();
        // The check for existence is against local cluster state, so very cheap
        if (hasLatestVersionedIndex(state)) {
            if (allPrimaryShardsActiveForLatestVersionedIndex(state)) {
                listener.onResponse(null);
                return;
            }
            // the index exists but is not ready yet
            waitForLatestVersionedIndexShardsActive(client, listener);
            return;
        }

        // Creating the index involves communication with the master node, so it's more expensive but much rarer
        try {
            CreateIndexRequest request = new CreateIndexRequest(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME).settings(
                settings(transformInternalIndexAdditionalSettings)
            )
                .mapping(mappings())
                .origin(TRANSFORM_ORIGIN)
                // explicitly wait for the primary shard (although this might be default)
                .waitForActiveShards(ActiveShardCount.ONE);
            ActionListener<CreateIndexResponse> innerListener = ActionListener.wrap(r -> listener.onResponse(null), e -> {
                // It's not a problem if the index already exists - another node could be running
                // this method at the same time as this one, and also have created the index
                // check if shards are active
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                    if (allPrimaryShardsActiveForLatestVersionedIndex(clusterService.state())) {
                        listener.onResponse(null);
                        return;
                    }
                    // the index exists but is not ready yet
                    waitForLatestVersionedIndexShardsActive(client, listener);
                } else {
                    listener.onFailure(e);
                }
            });
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                TRANSFORM_ORIGIN,
                request,
                innerListener,
                client.admin().indices()::create
            );
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private TransformInternalIndex() {}
}
