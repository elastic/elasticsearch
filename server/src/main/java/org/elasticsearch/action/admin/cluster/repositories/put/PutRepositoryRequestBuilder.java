/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.put;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

/**
 * Register repository request builder
 */
public class PutRepositoryRequestBuilder extends AcknowledgedRequestBuilder<
    PutRepositoryRequest,
    AcknowledgedResponse,
    PutRepositoryRequestBuilder> {

    /**
     * Constructs register repository request for the repository with a given name
     */
    public PutRepositoryRequestBuilder(ElasticsearchClient client, PutRepositoryAction action, String name) {
        super(client, action, new PutRepositoryRequest(name));
    }

    /**
     * Sets the repository type
     *
     * @param type repository type
     * @return this builder
     */
    public PutRepositoryRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings builder
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets whether or not repository should be verified after creation
     *
     * @param verify true if repository should be verified after registration, false otherwise
     * @return this builder
     */
    public PutRepositoryRequestBuilder setVerify(boolean verify) {
        request.verify(verify);
        return this;
    }
}
