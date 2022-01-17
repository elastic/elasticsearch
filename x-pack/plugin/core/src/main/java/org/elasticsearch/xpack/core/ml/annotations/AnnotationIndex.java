/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.annotations;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class AnnotationIndex {

    public static final String READ_ALIAS_NAME = ".ml-annotations-read";
    public static final String WRITE_ALIAS_NAME = ".ml-annotations-write";
    // Exposed for testing, but always use the aliases in non-test code
    public static final String INDEX_NAME = ".ml-annotations-6";

    private static final String MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";

    private static final Version HIDDEN_INTRODUCED_VERSION = Version.V_7_7_0;

    /**
     * Create the .ml-annotations-6 index with correct mappings if it does not already exist. This index is read and written by the UI
     * results views, so needs to exist when there might be ML results to view.  This method also waits for the index to be ready to search
     * before it returns.
     */
    public static void createAnnotationsIndexIfNecessaryAndWaitForYellow(
        Client client,
        ClusterState state,
        TimeValue masterNodeTimeout,
        final ActionListener<Boolean> finalListener
    ) {

        final ActionListener<Boolean> annotationsIndexCreatedListener = ActionListener.wrap(success -> {
            final ClusterHealthRequest request = Requests.clusterHealthRequest(READ_ALIAS_NAME)
                .waitForYellowStatus()
                .masterNodeTimeout(masterNodeTimeout);
            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                ClusterHealthAction.INSTANCE,
                request,
                ActionListener.wrap(r -> finalListener.onResponse(r.isTimedOut() == false), finalListener::onFailure)
            );
        }, finalListener::onFailure);

        createAnnotationsIndexIfNecessary(client, state, masterNodeTimeout, annotationsIndexCreatedListener);
    }

    /**
     * Create the .ml-annotations-6 index with correct mappings if it does not already exist. This index is read and written by the UI
     * results views, so needs to exist when there might be ML results to view.
     */
    public static void createAnnotationsIndexIfNecessary(
        Client client,
        ClusterState state,
        TimeValue masterNodeTimeout,
        final ActionListener<Boolean> finalListener
    ) {

        boolean isHiddenAttributeAvailable = state.nodes().getMinNodeVersion().onOrAfter(HIDDEN_INTRODUCED_VERSION);

        final ActionListener<Boolean> checkMappingsListener = ActionListener.wrap(
            success -> ElasticsearchMappings.addDocMappingIfMissing(
                WRITE_ALIAS_NAME,
                AnnotationIndex::annotationsMapping,
                client,
                state,
                masterNodeTimeout,
                finalListener
            ),
            finalListener::onFailure
        );

        final ActionListener<String> createAliasListener = ActionListener.wrap(currentIndexName -> {
            IndicesAliasesRequest.AliasActions addReadAliasAction = IndicesAliasesRequest.AliasActions.add()
                .index(currentIndexName)
                .alias(READ_ALIAS_NAME);
            IndicesAliasesRequest.AliasActions addWriteAliasAction = IndicesAliasesRequest.AliasActions.add()
                .index(currentIndexName)
                .alias(WRITE_ALIAS_NAME);
            if (isHiddenAttributeAvailable) {
                addReadAliasAction.isHidden(true);
                addWriteAliasAction.isHidden(true);
            }
            final IndicesAliasesRequest request = client.admin()
                .indices()
                .prepareAliases()
                .addAliasAction(addReadAliasAction)
                .addAliasAction(addWriteAliasAction)
                .request();
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ML_ORIGIN,
                request,
                ActionListener.<AcknowledgedResponse>wrap(
                    r -> checkMappingsListener.onResponse(r.isAcknowledged()),
                    finalListener::onFailure
                ),
                client.admin().indices()::aliases
            );
        }, finalListener::onFailure);

        // Only create the index or aliases if some other ML index exists - saves clutter if ML is never used.
        // Also, don't do this if there's a reset in progress or if ML upgrade mode is enabled.
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(state);
        SortedMap<String, IndexAbstraction> mlLookup = state.getMetadata().getIndicesLookup().tailMap(".ml");
        if (mlMetadata.isResetMode() == false
            && mlMetadata.isUpgradeMode() == false
            && mlLookup.isEmpty() == false
            && mlLookup.firstKey().startsWith(".ml")) {

            // Create the annotations index if it doesn't exist already.
            IndexAbstraction currentIndexAbstraction = mlLookup.get(INDEX_NAME);
            if (currentIndexAbstraction == null) {

                Settings.Builder settingsBuilder = Settings.builder()
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1");
                if (isHiddenAttributeAvailable) {
                    settingsBuilder.put(IndexMetadata.SETTING_INDEX_HIDDEN, true);
                }

                CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME).mapping(
                    SINGLE_MAPPING_NAME,
                    annotationsMapping(),
                    XContentType.JSON
                ).settings(settingsBuilder);

                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    ML_ORIGIN,
                    createIndexRequest,
                    ActionListener.<CreateIndexResponse>wrap(r -> createAliasListener.onResponse(INDEX_NAME), e -> {
                        // Possible that the index was created while the request was executing,
                        // so we need to handle that possibility
                        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                            // Create the alias
                            createAliasListener.onResponse(INDEX_NAME);
                        } else {
                            finalListener.onFailure(e);
                        }
                    }),
                    client.admin().indices()::create
                );
                return;
            }

            // Account for the possibility that the latest index has been reindexed
            // into a new index with the latest index name as an alias.
            String currentIndexName = currentIndexAbstraction.getIndices().get(0).getName();

            // Recreate the aliases if they've gone even though the index still exists.
            IndexAbstraction writeAliasAbstraction = mlLookup.get(WRITE_ALIAS_NAME);
            if (mlLookup.containsKey(READ_ALIAS_NAME) == false || writeAliasAbstraction == null) {
                createAliasListener.onResponse(currentIndexName);
                return;
            }

            List<Index> writeAliasIndices = writeAliasAbstraction.getIndices();
            if (writeAliasIndices.size() != 1 || currentIndexName.equals(writeAliasIndices.get(0).getName()) == false) {
                createAliasListener.onResponse(currentIndexName);
                return;
            }

            // Check the mappings
            checkMappingsListener.onResponse(false);
            return;
        }

        // Nothing to do, but respond to the listener
        finalListener.onResponse(false);
    }

    public static String annotationsMapping() {
        return annotationsMapping(SINGLE_MAPPING_NAME);
    }

    private static String annotationsMapping(String mappingType) {
        return TemplateUtils.loadTemplate(
            "/org/elasticsearch/xpack/core/ml/annotations_index_mappings.json",
            Version.CURRENT.toString(),
            MAPPINGS_VERSION_VARIABLE,
            Collections.singletonMap("xpack.ml.mapping_type", mappingType)
        );
    }
}
