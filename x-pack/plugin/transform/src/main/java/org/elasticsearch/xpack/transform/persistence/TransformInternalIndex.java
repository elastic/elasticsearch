/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
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

    // data types
    public static final String FLOAT = "float";
    public static final String DOUBLE = "double";
    public static final String LONG = "long";
    public static final String KEYWORD = "keyword";
    public static final String BOOLEAN = "boolean";

    public static IndexTemplateMetadata getIndexTemplateMetadata() throws IOException {
        IndexTemplateMetadata transformTemplate = IndexTemplateMetadata.builder(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME)
            .patterns(Collections.singletonList(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME))
            .version(Version.CURRENT.id)
            .settings(
                Settings.builder()
                    // the configurations are expected to be small
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            )
            .putMapping(MapperService.SINGLE_MAPPING_NAME, Strings.toString(mappings()))
            .build();
        return transformTemplate;
    }

    public static IndexTemplateMetadata getAuditIndexTemplateMetadata() throws IOException {
        IndexTemplateMetadata transformTemplate = IndexTemplateMetadata.builder(TransformInternalIndexConstants.AUDIT_INDEX)
            .patterns(Collections.singletonList(TransformInternalIndexConstants.AUDIT_INDEX_PREFIX + "*"))
            .version(Version.CURRENT.id)
            .settings(
                Settings.builder()
                    // the audits are expected to be small
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                    .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            )
            .putMapping(MapperService.SINGLE_MAPPING_NAME, Strings.toString(auditMappings()))
            .putAlias(AliasMetadata.builder(TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS).isHidden(true))
            .build();
        return transformTemplate;
    }

    private static XContentBuilder auditMappings() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.startObject(SINGLE_MAPPING_NAME);
        addMetaInformation(builder);
        builder.field(DYNAMIC, "false");
        builder
            .startObject(PROPERTIES)
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
        return builder
            .startObject(TransformStoredDoc.STATE_FIELD.getPreferredName())
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
        return builder
            .startObject(TransformField.ID.getPreferredName())
                .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TransformField.SOURCE.getPreferredName())
                .startObject(PROPERTIES)
                    .startObject(SourceConfig.INDEX.getPreferredName())
                        .field(TYPE, KEYWORD)
                    .endObject()
                    .startObject(SourceConfig.QUERY.getPreferredName())
                        .field(ENABLED, "false")
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
            .endObject();
    }

    private static XContentBuilder addTransformCheckpointMappings(XContentBuilder builder) throws IOException {
        return builder
            .startObject(TransformField.TIMESTAMP_MILLIS.getPreferredName())
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

    /**
     * This method should be called before any document is indexed that relies on the
     * existence of the latest index templates to create the internal and audit index.
     * The reason is that the standard template upgrader only runs when the master node
     * is upgraded to the newer version.  If data nodes are upgraded before master
     * nodes and transforms get assigned to those data nodes then without this check
     * the data nodes will index documents into the internal index before the necessary
     * index template is present and this will result in an index with completely
     * dynamic mappings being created (which is very bad).
     */
    public static void installLatestIndexTemplatesIfRequired(ClusterService clusterService, Client client, ActionListener<Void> listener) {

        installLatestVersionedIndexTemplateIfRequired(
            clusterService,
            client,
            ActionListener.wrap(r -> { installLatestAuditIndexTemplateIfRequired(clusterService, client, listener); }, listener::onFailure)
        );

    }

    protected static boolean haveLatestVersionedIndexTemplate(ClusterState state) {
        return state.getMetadata().getTemplates().containsKey(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME);
    }

    protected static boolean haveLatestAuditIndexTemplate(ClusterState state) {
        return state.getMetadata().getTemplates().containsKey(TransformInternalIndexConstants.AUDIT_INDEX);
    }

    protected static void installLatestVersionedIndexTemplateIfRequired(
        ClusterService clusterService,
        Client client,
        ActionListener<Void> listener
    ) {

        // The check for existence of the template is against local cluster state, so very cheap
        if (haveLatestVersionedIndexTemplate(clusterService.state())) {
            listener.onResponse(null);
            return;
        }

        // Installing the template involves communication with the master node, so it's more expensive but much rarer
        try {
            IndexTemplateMetadata indexTemplateMetadata = getIndexTemplateMetadata();
            BytesReference jsonMappings = new BytesArray(indexTemplateMetadata.mappings().uncompressed());
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME)
                .patterns(indexTemplateMetadata.patterns())
                .version(indexTemplateMetadata.version())
                .settings(indexTemplateMetadata.settings())
                .mapping(XContentHelper.convertToMap(jsonMappings, true, XContentType.JSON).v2());
            ActionListener<AcknowledgedResponse> innerListener = ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure);
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                TRANSFORM_ORIGIN,
                request,
                innerListener,
                client.admin().indices()::putTemplate
            );
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    protected static void installLatestAuditIndexTemplateIfRequired(
        ClusterService clusterService,
        Client client,
        ActionListener<Void> listener
    ) {

        // The check for existence of the template is against local cluster state, so very cheap
        if (haveLatestAuditIndexTemplate(clusterService.state())) {
            listener.onResponse(null);
            return;
        }

        // Installing the template involves communication with the master node, so it's more expensive but much rarer
        try {
            IndexTemplateMetadata indexTemplateMetadata = getAuditIndexTemplateMetadata();
            BytesReference jsonMappings = new BytesArray(indexTemplateMetadata.mappings().uncompressed());
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(TransformInternalIndexConstants.AUDIT_INDEX).patterns(
                indexTemplateMetadata.patterns()
            )
                .version(indexTemplateMetadata.version())
                .settings(indexTemplateMetadata.settings())
                .mapping(XContentHelper.convertToMap(jsonMappings, true, XContentType.JSON).v2());
            ActionListener<AcknowledgedResponse> innerListener = ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure);
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                TRANSFORM_ORIGIN,
                request,
                innerListener,
                client.admin().indices()::putTemplate
            );
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private TransformInternalIndex() {}
}
