/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.migration;

import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;

import java.util.Objects;

/**
 * Migration context for EIS (Elastic Inference Service) migrations.
 * Contains the authorization model which provides access to endpoint metadata
 * needed for batch migrations.
 */
public record ElasticInferenceServiceMigrationContext(ElasticInferenceServiceAuthorizationModel authModel) implements MigrationContext {

    public ElasticInferenceServiceMigrationContext {
        Objects.requireNonNull(authModel);
    }

    @Override
    public ContextType getContextType() {
        return ContextType.EIS;
    }
}
