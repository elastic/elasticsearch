/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.migration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Registry for managing all endpoint migrations across all inference services.
 * Supports both single-endpoint (lazy) and batch execution modes. Migrations are executed
 * in order from lowest to highest target version.
 *
 * <p>This registry is thread-safe.
 */
public class ModelMigrationRegistry {

    private static final Logger logger = LogManager.getLogger(ModelMigrationRegistry.class);

    private final CopyOnWriteArrayList<EndpointMigration> migrations;

    public ModelMigrationRegistry() {
        this.migrations = new CopyOnWriteArrayList<>();
    }

    /**
     * Registers a migration with the registry. Migrations are automatically sorted
     * by target version and indexed by applicable services.
     *
     * @param migration the migration to register
     * @throws IllegalArgumentException if migration is null
     */
    public void register(EndpointMigration migration) {
        migrations.add(migration);
        // Sort by target version to ensure execution order
        migrations.sort(Comparator.comparing(EndpointMigration::getTargetVersion));

        // Index by service for faster lookup
        for (String service : migration.getApplicableServices()) {
            migrationsByService.computeIfAbsent(service, k -> new CopyOnWriteArrayList<>()).add(migration);
            migrationsByService.computeIfAbsent(service, k -> new CopyOnWriteArrayList<>()).add(migration);
            // Keep service-specific list sorted too
            List<EndpointMigration> serviceMigrations = migrationsByService.get(service);
            serviceMigrations.sort(Comparator.comparingLong(EndpointMigration::getTargetVersion));
        }

        logger.debug(
            "Registered migration with target version [{}] for services [{}]",
            migration.getTargetVersion(),
            migration.getApplicableServices()
        );
    }

    /**
     * Executes batch migrations for multiple endpoints. Only migrations that support
     * batch mode and are applicable to the provided models are executed. Batch migrations
     * receive the migration context (e.g., EISMigrationContext for authorization data).
     *
     * @param models the models to migrate
     * @param currentVersions map of model inference entity ID to current version
     * @param context optional migration context (typically provided for batch migrations)
     * @return list of migration results, one per model
     */
    public List<EndpointMigration.MigrationResult> executeBatchMigrations(
        List<Model> models,
        java.util.Map<String, Integer> currentVersions,
        @Nullable MigrationContext context
    ) {
        Objects.requireNonNull(models, "Models list cannot be null");
        Objects.requireNonNull(currentVersions, "Current versions map cannot be null");

        if (models.isEmpty()) {
            return Collections.emptyList();
        }

        // Create a map to track results for each model
        java.util.Map<String, EndpointMigration.MigrationResult> resultsByModelId = new java.util.HashMap<>();

        // Initialize with success results for all models (will be updated if migrations are needed)
        for (Model model : models) {
            resultsByModelId.put(model.getInferenceEntityId(), EndpointMigration.MigrationResult.success(model));
        }

        // Group models by service for efficient batch processing
        java.util.Map<String, List<Model>> modelsByService = models.stream()
            .collect(Collectors.groupingBy(model -> model.getConfigurations().getService()));

        for (java.util.Map.Entry<String, List<Model>> entry : modelsByService.entrySet()) {
            String service = entry.getKey();
            List<Model> serviceModels = new ArrayList<>(entry.getValue());

            // Get batch-capable migrations for this service
            List<EndpointMigration> batchMigrations = migrations.stream()
                .filter(m -> m.supportsBatch())
                .filter(m -> m.getApplicableServices().contains(service))
                .sorted(Comparator.comparingLong(EndpointMigration::getTargetVersion))
                .collect(Collectors.toList());

            if (batchMigrations.isEmpty()) {
                continue; // No batch migrations, keep existing success results
            }

            // Execute batch migrations for this service's models
            for (EndpointMigration migration : batchMigrations) {
                try {
                    // Filter models that need this migration
                    List<Model> modelsNeedingMigration = serviceModels.stream().filter(model -> {
                        int currentVersion = currentVersions.getOrDefault(model.getInferenceEntityId(), 0);
                        return currentVersion < migration.getTargetVersion() && migration.isApplicable(model);
                    }).collect(Collectors.toList());

                    if (modelsNeedingMigration.isEmpty()) {
                        continue;
                    }

                    logger.debug(
                        "Executing batch migration [{}] for [{}] models in service [{}]",
                        migration.getTargetVersion(),
                        modelsNeedingMigration.size(),
                        service
                    );

                    List<EndpointMigration.MigrationResult> batchResults = migration.migrateBatch(modelsNeedingMigration, context);

                    // Update results and models for successful migrations
                    for (int i = 0; i < modelsNeedingMigration.size() && i < batchResults.size(); i++) {
                        Model originalModel = modelsNeedingMigration.get(i);
                        EndpointMigration.MigrationResult result = batchResults.get(i);

                        // Update the result for this model
                        resultsByModelId.put(originalModel.getInferenceEntityId(), result);

                        // Update the model in the list for subsequent migrations if successful
                        if (result.success() && result.migratedModel() != null) {
                            int index = serviceModels.indexOf(originalModel);
                            if (index >= 0) {
                                serviceModels.set(index, result.migratedModel().model());
                            }
                        }
                    }

                } catch (Exception e) {
                    logger.error(
                        () -> org.elasticsearch.core.Strings.format(
                            "Exception during batch migration [%d] for service [%s]",
                            migration.getTargetVersion(),
                            service
                        ),
                        e
                    );
                    // Update results for models that were being migrated
                    for (Model model : serviceModels) {
                        int currentVersion = currentVersions.getOrDefault(model.getInferenceEntityId(), 0);
                        if (currentVersion < migration.getTargetVersion() && migration.isApplicable(model)) {
                            resultsByModelId.put(
                                model.getInferenceEntityId(),
                                EndpointMigration.MigrationResult.failure(
                                    "Batch migration [%d] failed: %s",
                                    migration.getTargetVersion(),
                                    e.getMessage()
                                )
                            );
                        }
                    }
                }
            }
        }

        // Return results in the same order as input models
        return models.stream()
            .map(model -> resultsByModelId.get(model.getInferenceEntityId()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /**
     * Gets all migrations applicable to a model that target a version higher than the current version.
     *
     * @param model the model to check
     * @param service the service name
     * @param currentVersion the current version of the model
     * @return list of applicable migrations, sorted by target version
     */
    private List<EndpointMigration> getApplicableMigrations(Model model, String service, int currentVersion) {
        List<EndpointMigration> serviceMigrations = migrationsByService.getOrDefault(service, Collections.emptyList());

        return serviceMigrations.stream()
            .filter(migration -> migration.isApplicable(model))
            .filter(migration -> migration.getTargetVersion() > currentVersion)
            .collect(Collectors.toList());
    }

    /**
     * Returns all registered migrations. The returned list is a snapshot and is not
     * affected by subsequent registrations.
     *
     * @return unmodifiable list of all registered migrations
     */
    public List<EndpointMigration> getAllMigrations() {
        return Collections.unmodifiableList(new ArrayList<>(migrations));
    }

    /**
     * Returns all migrations for a specific service. The returned list is a snapshot
     * and is not affected by subsequent registrations.
     *
     * @param service the service name
     * @return unmodifiable list of migrations for the service
     */
    public List<EndpointMigration> getMigrationsForService(String service) {
        List<EndpointMigration> serviceMigrations = migrationsByService.getOrDefault(service, Collections.emptyList());
        return Collections.unmodifiableList(new ArrayList<>(serviceMigrations));
    }
}
