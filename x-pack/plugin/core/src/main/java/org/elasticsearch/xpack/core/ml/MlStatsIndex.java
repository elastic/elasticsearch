/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.template.TemplateUtils;

/**
 * Describes the indices where ML is storing various stats about the users jobs.
 */
public class MlStatsIndex {

    public static final String TEMPLATE_NAME = ".ml-stats";

    private static final String MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";

    private MlStatsIndex() {}

    public static String mapping() {
        return TemplateUtils.loadTemplate("/org/elasticsearch/xpack/core/ml/stats_index_mappings.json",
            Version.CURRENT.toString(), MAPPINGS_VERSION_VARIABLE);
    }

    public static String indexPattern() {
        return TEMPLATE_NAME + "-*";
    }

    public static String writeAlias() {
        return ".ml-stats-write";
    }

    /**
     * Creates the first concrete .ml-stats-000001 index (if necessary)
     * Creates the .ml-stats-write alias for that index.
     */
    public static void createStatsIndexAndAliasIfNecessary(Client client, ClusterState state, ActionListener<Boolean> listener) {

        if (state.getMetaData().getAliasAndIndexLookup().containsKey(writeAlias())) {
            listener.onResponse(false);
            return;
        }

        ActionListener<CreateIndexResponse> createIndexListener = ActionListener.wrap(
            createIndexResponse -> listener.onResponse(true),
            error -> {
                if (ExceptionsHelper.unwrapCause(error) instanceof ResourceAlreadyExistsException) {
                    listener.onResponse(true);
                } else {
                    listener.onFailure(error);
                }
            }
        );

        CreateIndexRequest createIndexRequest = client.admin()
            .indices()
            .prepareCreate(TEMPLATE_NAME + "-000001")
            .addAlias(new Alias(writeAlias()).writeIndex(true))
            .request();

        ClientHelper.executeAsyncWithOrigin(client,
            ClientHelper.ML_ORIGIN,
            CreateIndexAction.INSTANCE,
            createIndexRequest,
            createIndexListener);
    }
}
