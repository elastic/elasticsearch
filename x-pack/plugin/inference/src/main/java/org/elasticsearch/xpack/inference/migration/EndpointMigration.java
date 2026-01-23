/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.migration;

import org.apache.maven.artifact.versioning.ComparableVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.Model;

import java.util.List;

/**
 * Unified interface for endpoint migrations supporting both single-endpoint
 * and batch execution modes. Migrations can be run lazily when endpoints are
 * read (single-endpoint) or efficiently during batch operations (batch mode).
 */
public interface EndpointMigration {

    /**
     * Returns the target version this migration migrates endpoints to.
     * Migrations are executed in order from lowest to highest target version.
     *
     * @return the target version number
     */
    ComparableVersion getTargetVersion();

    /**
     * Returns whether this migration supports batch execution mode.
     * Batch migrations can process multiple endpoints efficiently with access
     * to migration context data (e.g., authorization responses).
     *
     * @return true if batch mode is supported, false otherwise
     */
    default boolean supportsBatch() {
        return false;
    }

    /**
     * Migrates a single endpoint. This method is called for single-endpoint
     * migrations when endpoints are read (lazy migration).
     *
     * @param model the model to migrate
     * @param context optional migration context (may be null for single-endpoint migrations)
     * @return the migration result
     */
    MigrationResult migrate(Model model, @Nullable MigrationContext context);

    /**
     * Migrates multiple endpoints in batch mode. This method is called when
     * batch migrations are supported and context data is available.
     *
     * @param models the models to migrate
     * @param context optional migration context (typically provided for batch migrations)
     * @return list of migration results, one per model
     */
    default List<MigrationResult> migrateBatch(List<Model> models, @Nullable MigrationContext context) {
        throw new UnsupportedOperationException("Batch migration not supported by this migration");
    }

    /**
     * Record representing a successfully migrated model.
     *
     * @param model the migrated model
     */
    record MigratedModel(Model model) {
        public MigratedModel {
            if (model == null) {
                throw new IllegalArgumentException("Migrated model cannot be null");
            }
        }
    }

    /**
     * Record representing the result of a migration operation.
     *
     * @param success whether the migration succeeded
     * @param migratedModel the migrated model if successful, null otherwise
     * @param errorMessage error message if migration failed, null otherwise
     */
    record MigrationResult(boolean success, @Nullable MigratedModel migratedModel, @Nullable String errorMessage) {

        /**
         * Creates a successful migration result.
         *
         * @param model the migrated model
         * @return successful migration result
         */
        public static MigrationResult success(Model model) {
            return new MigrationResult(true, new MigratedModel(model), null);
        }

        /**
         * Creates a failed migration result.
         *
         * @param errorMessage the error message describing the failure
         * @return failed migration result
         */
        public static MigrationResult failure(String errorMessage) {
            return new MigrationResult(false, null, errorMessage);
        }

        /**
         * Creates a failed migration result with a formatted error message.
         *
         * @param format the format string
         * @param args the format arguments
         * @return failed migration result
         */
        public static MigrationResult failure(String format, Object... args) {
            return failure(Strings.format(format, args));
        }
    }
}
