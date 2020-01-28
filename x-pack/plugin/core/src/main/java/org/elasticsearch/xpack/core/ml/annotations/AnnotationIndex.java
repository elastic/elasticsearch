/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.annotations;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.SortedMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class AnnotationIndex {

    public static final String READ_ALIAS_NAME = ".ml-annotations-read";
    public static final String WRITE_ALIAS_NAME = ".ml-annotations-write";
    // Exposed for testing, but always use the aliases in non-test code
    public static final String INDEX_NAME = ".ml-annotations-6";
    public static final String INDEX_PATTERN = ".ml-annotations*";

    /**
     * Create the .ml-annotations index with correct mappings if it does not already
     * exist. This index is read and written by the UI results views, so needs to
     * exist when there might be ML results to view.
     */
    public static void createAnnotationsIndexIfNecessary(Settings settings, Client client, ClusterState state,
                                                         final ActionListener<Boolean> finalListener) {

        final ActionListener<Boolean> createAliasListener = ActionListener.wrap(success -> {
            final IndicesAliasesRequest request = client.admin().indices().prepareAliases()
                .addAlias(INDEX_NAME, READ_ALIAS_NAME)
                .addAlias(INDEX_NAME, WRITE_ALIAS_NAME).request();
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, request,
                ActionListener.<AcknowledgedResponse>wrap(r -> finalListener.onResponse(r.isAcknowledged()), finalListener::onFailure),
                client.admin().indices()::aliases);
        }, finalListener::onFailure);

        // Only create the index or aliases if some other ML index exists - saves clutter if ML is never used.
        SortedMap<String, AliasOrIndex> mlLookup = state.getMetaData().getAliasAndIndexLookup().tailMap(".ml");
        if (mlLookup.isEmpty() == false && mlLookup.firstKey().startsWith(".ml")) {

            // Create the annotations index if it doesn't exist already.
            if (mlLookup.containsKey(INDEX_NAME) == false) {

                final TimeValue delayedNodeTimeOutSetting;
                // Whether we are using native process is a good way to detect whether we are in dev / test mode:
                if (MachineLearningField.AUTODETECT_PROCESS.get(settings)) {
                    delayedNodeTimeOutSetting = UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(settings);
                } else {
                    delayedNodeTimeOutSetting = TimeValue.ZERO;
                }

                CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
                try (XContentBuilder annotationsMapping = AnnotationIndex.annotationsMapping()) {
                    createIndexRequest.mapping(annotationsMapping);
                    createIndexRequest.settings(Settings.builder()
                        .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                        .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting));

                    executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, createIndexRequest,
                        ActionListener.<CreateIndexResponse>wrap(
                            r -> createAliasListener.onResponse(r.isAcknowledged()),
                            e -> {
                                // Possible that the index was created while the request was executing,
                                // so we need to handle that possibility
                                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                                    // Create the alias
                                    createAliasListener.onResponse(true);
                                } else {
                                    finalListener.onFailure(e);
                                }
                            }
                        ), client.admin().indices()::create);
                } catch (IOException e) {
                    finalListener.onFailure(e);
                }
                return;
            }

            // Recreate the aliases if they've gone even though the index still exists.
            if (mlLookup.containsKey(READ_ALIAS_NAME) == false || mlLookup.containsKey(WRITE_ALIAS_NAME) == false) {
                createAliasListener.onResponse(true);
                return;
            }
        }

        // Nothing to do, but respond to the listener
        finalListener.onResponse(false);
    }

    public static XContentBuilder annotationsMapping() throws IOException {
        XContentBuilder builder = jsonBuilder()
            .startObject()
                .startObject(SINGLE_MAPPING_NAME);
        ElasticsearchMappings.addMetaInformation(builder);
        builder.startObject(ElasticsearchMappings.PROPERTIES)
                        .startObject(Annotation.ANNOTATION.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.TEXT)
                        .endObject()
                        .startObject(Annotation.CREATE_TIME.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.DATE)
                        .endObject()
                        .startObject(Annotation.CREATE_USERNAME.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.KEYWORD)
                        .endObject()
                        .startObject(Annotation.TIMESTAMP.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.DATE)
                        .endObject()
                        .startObject(Annotation.END_TIMESTAMP.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.DATE)
                        .endObject()
                        .startObject(Job.ID.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.KEYWORD)
                        .endObject()
                        .startObject(Annotation.MODIFIED_TIME.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.DATE)
                        .endObject()
                        .startObject(Annotation.MODIFIED_USERNAME.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.KEYWORD)
                        .endObject()
                        .startObject(Annotation.TYPE.getPreferredName())
                            .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.KEYWORD)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        return builder;
    }
}
