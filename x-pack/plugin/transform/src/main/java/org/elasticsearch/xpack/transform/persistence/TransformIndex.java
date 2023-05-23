/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.DestAlias;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public final class TransformIndex {
    private static final Logger logger = LogManager.getLogger(TransformIndex.class);

    private static final String PROPERTIES = "properties";
    private static final String META = "_meta";

    private TransformIndex() {}

    /**
     * Checks whether the given index was created automatically by the transform rather than explicitly by the user.
     *
     * @param client ES client
     * @param destIndex Destination index name/pattern
     * @param listener Listener to be called after the check is done.
     *                 Returns {@code true} if the given index was created by the transform and {@code false} otherwise.
     */
    public static void isDestinationIndexCreatedByTransform(Client client, String destIndex, ActionListener<Boolean> listener) {
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(destIndex)
            // We only need mappings, more specifically its "_meta" part
            .features(GetIndexRequest.Feature.MAPPINGS);
        executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, GetIndexAction.INSTANCE, getIndexRequest, ActionListener.wrap(getIndexResponse -> {
            Map<String, MappingMetadata> indicesMappings = getIndexResponse.mappings();
            if (indicesMappings.containsKey(destIndex) == false) {
                listener.onResponse(false);
                return;
            }
            Map<String, Object> indexMapping = indicesMappings.get(destIndex).getSourceAsMap();
            if (indexMapping.containsKey(META) == false) {
                listener.onResponse(false);
                return;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> indexMappingMetadata = (Map<String, Object>) indexMapping.get(META);
            if (indexMappingMetadata.containsKey(TransformField.CREATED_BY) == false) {
                listener.onResponse(false);
                return;
            }
            String createdBy = (String) indexMappingMetadata.get(TransformField.CREATED_BY);
            if (TransformField.TRANSFORM_SIGNATURE.equals(createdBy) == false) {
                listener.onResponse(false);
                return;
            }
            listener.onResponse(true);
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                // Missing index is ok, we should report that it was not created by transform (because it doesn't exist)
                listener.onResponse(false);
                return;
            }
            listener.onFailure(e);
        }));
    }

    public static void createDestinationIndex(
        Client client,
        TransformAuditor auditor,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        TransformConfig config,
        Map<String, String> destIndexMappings,
        ActionListener<Boolean> listener
    ) {
        final String destinationIndex = config.getDestination().getIndex();
        String[] dest = indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), destinationIndex);

        // <3> Final listener
        ActionListener<Boolean> setUpDestinationAliasesListener = ActionListener.wrap(r -> {
            String message = "Set up aliases ["
                + config.getDestination().getAliases().stream().map(DestAlias::getAlias).collect(joining(", "))
                + "] for destination index ["
                + destinationIndex
                + "].";
            auditor.info(config.getId(), message);

            listener.onResponse(r);
        }, listener::onFailure);

        // <2> Set up destination index aliases, regardless whether the destination index was created by the transform or by the user
        ActionListener<Boolean> createDestinationIndexListener = ActionListener.wrap(createdDestinationIndex -> {
            if (createdDestinationIndex) {
                String message = Boolean.FALSE.equals(config.getSettings().getDeduceMappings())
                    ? "Created destination index [" + destinationIndex + "]."
                    : "Created destination index [" + destinationIndex + "] with deduced mappings.";
                auditor.info(config.getId(), message);
            }
            setUpDestinationAliases(client, config, setUpDestinationAliasesListener);
        }, listener::onFailure);

        if (dest.length == 0) {
            TransformDestIndexSettings generatedDestIndexSettings = createTransformDestIndexSettings(
                destIndexMappings,
                config.getId(),
                Clock.systemUTC()
            );

            // <1> Create destination index
            createDestinationIndex(client, config, generatedDestIndexSettings, createDestinationIndexListener);
        } else {
            auditor.info(config.getId(), "Using existing destination index [" + destinationIndex + "].");
            // <1'> Audit existing destination index' stats
            ClientHelper.executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ClientHelper.TRANSFORM_ORIGIN,
                client.admin().indices().prepareStats(dest).clear().setDocs(true).request(),
                ActionListener.<IndicesStatsResponse>wrap(r -> {
                    long docTotal = r.getTotal().docs.getCount();
                    if (docTotal > 0L) {
                        auditor.warning(
                            config.getId(),
                            "Non-empty destination index [" + destinationIndex + "]. " + "Contains [" + docTotal + "] total documents."
                        );
                    }
                    createDestinationIndexListener.onResponse(false);
                }, e -> {
                    String message = "Unable to determine destination index stats, error: " + e.getMessage();
                    logger.warn(message, e);
                    auditor.warning(config.getId(), message);
                    createDestinationIndexListener.onResponse(false);
                }),
                client.admin().indices()::stats
            );
        }
    }

    static void createDestinationIndex(
        Client client,
        TransformConfig config,
        TransformDestIndexSettings destIndexSettings,
        ActionListener<Boolean> listener
    ) {
        CreateIndexRequest request = new CreateIndexRequest(config.getDestination().getIndex());
        request.settings(destIndexSettings.getSettings());
        request.mapping(destIndexSettings.getMappings());
        for (Alias alias : destIndexSettings.getAliases()) {
            request.alias(alias);
        }

        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(),
            TRANSFORM_ORIGIN,
            client,
            CreateIndexAction.INSTANCE,
            request,
            ActionListener.wrap(createIndexResponse -> {
                listener.onResponse(true);
            }, e -> {
                if (e instanceof ResourceAlreadyExistsException) {
                    // Already existing index is ok, it could have been created by the indexing process of the running transform.
                    listener.onResponse(false);
                    return;
                }
                String message = TransformMessages.getMessage(
                    TransformMessages.FAILED_TO_CREATE_DESTINATION_INDEX,
                    config.getDestination().getIndex(),
                    config.getId()
                );
                logger.error(message, e);
                listener.onFailure(new RuntimeException(message, e));
            })
        );
    }

    static void setUpDestinationAliases(Client client, TransformConfig config, ActionListener<Boolean> listener) {
        // No aliases configured to be set up, just move on
        if (config.getDestination().getAliases() == null || config.getDestination().getAliases().isEmpty()) {
            listener.onResponse(true);
            return;
        }

        IndicesAliasesRequest request = new IndicesAliasesRequest();
        for (DestAlias destAlias : config.getDestination().getAliases()) {
            if (destAlias.isMoveOnCreation()) {
                request.addAliasAction(IndicesAliasesRequest.AliasActions.remove().alias(destAlias.getAlias()).index("*"));
            }
        }
        for (DestAlias destAlias : config.getDestination().getAliases()) {
            request.addAliasAction(
                IndicesAliasesRequest.AliasActions.add().alias(destAlias.getAlias()).index(config.getDestination().getIndex())
            );
        }

        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(),
            TRANSFORM_ORIGIN,
            client,
            IndicesAliasesAction.INSTANCE,
            request,
            ActionListener.wrap(aliasesResponse -> {
                listener.onResponse(true);
            }, e -> {
                String message = TransformMessages.getMessage(
                    TransformMessages.FAILED_TO_SET_UP_DESTINATION_ALIASES,
                    config.getDestination().getIndex(),
                    config.getId()
                );
                logger.error(message, e);
                listener.onFailure(new RuntimeException(message, e));
            })
        );
    }

    public static TransformDestIndexSettings createTransformDestIndexSettings(Map<String, String> mappings, String id, Clock clock) {
        Map<String, Object> indexMappings = new HashMap<>();
        indexMappings.put(PROPERTIES, createMappingsFromStringMap(mappings));
        indexMappings.put(META, createMetadata(id, clock));

        Settings settings = createSettings();

        // transform does not create aliases, however the user might customize this in future
        Set<Alias> aliases = null;
        return new TransformDestIndexSettings(indexMappings, settings, aliases);
    }

    /*
     * Return meta data that stores some useful information about the transform index, stored as "_meta":
     *
     * {
     *   "created_by" : "transform",
     *   "_transform" : {
     *     "transform" : "id",
     *     "version" : {
     *       "created" : "8.0.0"
     *     },
     *     "creation_date_in_millis" : 1584025695202
     *   }
     * }
     */
    private static Map<String, Object> createMetadata(String id, Clock clock) {

        Map<String, Object> metadata = new HashMap<>();
        metadata.put(TransformField.CREATED_BY, TransformField.TRANSFORM_SIGNATURE);

        Map<String, Object> transformMetadata = new HashMap<>();
        transformMetadata.put(TransformField.CREATION_DATE_MILLIS, clock.millis());
        transformMetadata.put(TransformField.VERSION.getPreferredName(), Map.of(TransformField.CREATED, Version.CURRENT.toString()));
        transformMetadata.put(TransformField.TRANSFORM, id);

        metadata.put(TransformField.META_FIELDNAME, transformMetadata);
        return metadata;
    }

    /**
     * creates generated index settings, hardcoded at the moment, in future this might be customizable or generation could
     * be based on source settings.
     */
    private static Settings createSettings() {
        return Settings.builder() // <1>
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    /**
     * This takes the a {@code Map<String, String>} of the type "fieldname: fieldtype" and transforms it into the
     * typical mapping format.
     *
     * Example:
     *
     * input:
     * {"field1.subField1": "long", "field2": "keyword"}
     *
     * output:
     * {
     *   "field1.subField1": {
     *     "type": "long"
     *   },
     *   "field2": {
     *     "type": "keyword"
     *   }
     * }
     * @param mappings A Map of the form {"fieldName": "fieldType"}
     */
    static Map<String, Object> createMappingsFromStringMap(Map<String, String> mappings) {
        return mappings.entrySet().stream().collect(toMap(e -> e.getKey(), e -> singletonMap("type", e.getValue())));
    }
}
