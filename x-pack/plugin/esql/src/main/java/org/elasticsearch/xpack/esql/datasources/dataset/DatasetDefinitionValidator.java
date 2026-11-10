/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.DatasetReference;
import org.elasticsearch.xpack.esql.datasources.DeclaredSchemaValidator;
import org.elasticsearch.xpack.esql.datasources.PartitionSpec;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.Map;

/**
 * Decides what a dataset definition means, or refuses it: which data source it reads through, which settings survive
 * that source's own validator, and what the declared mapping does to them. Everything a definition must satisfy is
 * here, so the two callers — a registration and an inline read — cannot come to disagree. What <em>registering</em>
 * additionally requires (a free name, the ceiling) stays with the registration.
 */
public final class DatasetDefinitionValidator {

    /** Never read: the request is validated in process and never sent. Supplied so its non-null invariant holds. */
    private static final TimeValue UNUSED_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final Map<String, DataSourceValidator> validatorsByType;

    public DatasetDefinitionValidator(Map<String, DataSourceValidator> validatorsByType) {
        this.validatorsByType = Map.copyOf(validatorsByType);
    }

    /** A validated definition together with the data source it was validated against, which its query config needs. */
    public record BoundDataset(DataSource dataSource, Dataset dataset) {}

    /**
     * The {@link Dataset} this definition denotes, validated against {@code project} as the caller has it.
     *
     * @throws ResourceNotFoundException if the definition names a data source this project does not have
     * @throws ValidationException if a setting shadows one of that source's secrets
     * @throws IllegalArgumentException if the source's own validator refuses a setting, or the mapping is not declarable
     */
    public Dataset validate(ProjectMetadata project, PutDatasetAction.Request request) {
        return bind(project, request).dataset();
    }

    /**
     * The same validation for a definition read inline, via the request a registration would have made. Its
     * {@code validate()} is run here because, unlike a REST registration, nothing in front of this will run it.
     */
    public BoundDataset validateInline(ProjectMetadata project, DatasetReference.Inline inline) {
        PutDatasetAction.Request request = inline.asRegistrationRequest(UNUSED_TIMEOUT);
        ActionRequestValidationException invalid = request.validate();
        if (invalid != null) {
            throw invalid;
        }
        return bind(project, request);
    }

    /** The data source a definition reads through, as {@code project} has it. */
    public DataSource requireDataSource(ProjectMetadata project, String name) {
        DataSource parent = DataSourceMetadata.get(project).get(name);
        if (parent == null) {
            throw new ResourceNotFoundException("data source [{}] not found", name);
        }
        return parent;
    }

    private BoundDataset bind(ProjectMetadata project, PutDatasetAction.Request request) {
        final DataSource parent = requireDataSource(project, request.dataSource());
        final DataSourceValidator validator = validatorsByType.get(parent.type());
        if (validator == null) {
            throw new IllegalStateException("no validator registered for data source type [" + parent.type() + "]");
        }
        // Pre-validator shadow check: if a raw setting key matches a parent secret, report the
        // specific shadow-rejection message rather than the generic "unknown setting [key]" the
        // format validator would produce when it sees an unrecognised key.
        if (request.rawSettings() != null) {
            for (String key : request.rawSettings().keySet()) {
                DataSourceSetting parentSetting = parent.settings().get(key);
                if (parentSetting != null && parentSetting.secret()) {
                    throwShadowError(key);
                }
            }
        }
        final Map<String, Object> validatedSettings = validator.validateDataset(
            parent.settings().asMap(),
            request.resource(),
            request.rawSettings()
        );

        // Shape-only validation of the declared mapping (no file I/O): declarable types and rename name collisions.
        // A `path` column rename is honored by all formats (translation is centralized at the reader boundary).
        DeclaredSchemaValidator.validate(request.mapping());
        Map<String, Object> settings = PartitionSpec.alignWithMapping(validatedSettings, request.mapping());
        // Over the map the Dataset carries: catches a validator that RETURNS a shadowing key the request never sent.
        for (String key : settings.keySet()) {
            DataSourceSetting parentSetting = parent.settings().get(key);
            if (parentSetting != null && parentSetting.secret()) {
                throwShadowError(key);
            }
        }
        return new BoundDataset(
            parent,
            new Dataset(
                request.name(),
                new DataSourceReference(request.dataSource()),
                request.resource(),
                request.description(),
                settings,
                request.mapping()
            )
        );
    }

    private static void throwShadowError(String key) {
        ValidationException ex = new ValidationException();
        ex.addValidationError("dataset setting [" + key + "] shadows a secret data-source setting; remove from dataset settings");
        throw ex;
    }
}
