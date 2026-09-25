/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;

/**
 * Closed vocabularies and reason mapping for {@code config.changes.total}. Lives outside {@code spi}
 * so it can see the package-local max-count / unknown-type exceptions.
 */
public final class ConfigChangeTelemetry {

    public static final String KIND_DATASOURCE = "datasource";
    public static final String KIND_DATASET = "dataset";

    public static final String OP_CREATED = "created";
    public static final String OP_UPDATED = "updated";
    public static final String OP_DELETED = "deleted";
    public static final String OP_REJECTED = "rejected";

    public static final String REASON_VALIDATION = "validation";
    public static final String REASON_NOT_FOUND = "not_found";
    public static final String REASON_ALREADY_EXISTS = "already_exists";
    public static final String REASON_MAX_COUNT = "max_count";
    public static final String REASON_UNKNOWN_TYPE = "unknown_type";
    public static final String REASON_UNAVAILABLE = "unavailable";
    public static final String REASON_HAS_DEPENDENTS = "has_dependents";
    public static final String REASON_OTHER = "other";

    private ConfigChangeTelemetry() {}

    /**
     * Clamps a validator type-id to the closed {@link Type} set used by both CRUD APM
     * ({@code es_datasource_type}) and phone-home inventory ({@code by_type}).
     * Scheme aliases such as {@code file} are not type ids and become {@code unknown}.
     */
    public static String typeToken(String type) {
        return Type.fromTypeId(type).key();
    }

    /**
     * Maps a terminal refusal to a closed reason, or {@code null} when the failure is a publish
     * retry ({@link MasterService#isPublishFailureException}) and must not be counted.
     */
    public static String rejectedReason(Exception e) {
        if (e == null || MasterService.isPublishFailureException(e)) {
            return null;
        }
        if (e instanceof ValidationException) {
            return REASON_VALIDATION;
        }
        if (e instanceof ResourceNotFoundException) {
            return REASON_NOT_FOUND;
        }
        if (e instanceof ResourceAlreadyExistsException) {
            return REASON_ALREADY_EXISTS;
        }
        if (e instanceof UnknownDataSourceTypeException) {
            return REASON_UNKNOWN_TYPE;
        }
        if (e instanceof MaxDataSourcesCountException || e instanceof MaxDatasetsCountException) {
            return REASON_MAX_COUNT;
        }
        if (e instanceof ElasticsearchStatusException ese && ese.status() == RestStatus.SERVICE_UNAVAILABLE) {
            return REASON_UNAVAILABLE;
        }
        if (e instanceof ElasticsearchStatusException ese && ese.status() == RestStatus.CONFLICT) {
            return REASON_HAS_DEPENDENTS;
        }
        // Dedicated IAE subtypes are matched above; leftover IAE is PUT validation
        // (DeclaredSchemaValidator, ConfigKeyValidator, and similar).
        if (e instanceof IllegalArgumentException) {
            return REASON_VALIDATION;
        }
        return REASON_OTHER;
    }

    public static void recordRejected(ExternalSourceMetrics metrics, String kind, String type, Exception e) {
        String reason = rejectedReason(e);
        if (reason != null) {
            metrics.recordConfigChange(kind, OP_REJECTED, typeToken(type), reason);
        }
    }
}
