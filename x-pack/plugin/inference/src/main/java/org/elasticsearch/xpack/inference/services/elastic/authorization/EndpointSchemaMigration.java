/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.inference.metadata.EndpointMetadata;

/**
 * Placeholder class for endpoint migration logic. When a new field is added to the Elastic Inference Service authorization API schema,
 * we will need to migrate existing endpoints. To do this we will retrieve the latest preconfigured endpoints for the authorization API
 * and update the endpoints that were already stored in the index. If the endpoint is new we can simply store it instead of updating it.
 * <p>
 * To determine if an endpoint is new or existing, we will use the internal version field within {@link EndpointMetadata#internal()}.
 * We will compare the version field of the existing endpoint with the value of {@link #ENDPOINT_SCHEMA_VERSION}.
 * If the endpoint's version is less than the current version, we will migrate the endpoint to the latest schema.
 * If it is the same (or greater for some reason), we will leave it as is.
 */
public class EndpointSchemaMigration {

    /**
     * The current version of the endpoint schema. This should be incremented each time we need to handle a new field added to the
     * authorization API schema. Incrementing this version will cause existing endpoints to be migrated to the latest schema by
     * retrieving the latest preconfigured endpoints and updating existing ones to include any new fields included in the authorization
     * response.
     */
    public static final Long ENDPOINT_SCHEMA_VERSION = 0L;

    private EndpointSchemaMigration() {}
}
