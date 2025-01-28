/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.persistence;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Encapsulates a single migration action.
 */
public interface Migration {
    /**
     * @return The index template version that the targeted index will be on after the migration has been applied.
     */
    int getTargetIndexTemplateVersion();

    /**
     * Applies this migration. Depending on the type of migration one or the other consumer will be called.
     *
     * @param index The name of the specific index to which the migration should be applied.
     * @param mappingConsumer A consumer that applies mapping changes.
     * @param settingsConsumer A consumer that applies settings changes.
     */
    void apply(String index, Consumer<PutMappingRequest> mappingConsumer, Consumer<UpdateSettingsRequest> settingsConsumer);

    /**
     * Builds migrations for an index.
     */
    class Builder {
        private final List<Migration> migrations = new ArrayList<>();
        private Integer targetVersion;

        private void checkVersionSet() {
            if (targetVersion == null) {
                throw new IllegalStateException("Set targetVersion before defining migrations");
            }
        }

        /**
         * @param version The index template version that the targeted index will reach after this migration has been applied.
         * @return <code>this</code> to allow for method chaining.
         */
        public Builder migrateToIndexTemplateVersion(int version) {
            this.targetVersion = version;
            return this;
        }

        /**
         * Adds new property to an index mapping. This method should be used for simple cases where a new property needs to be added
         * without any further mapping parameters. For more complex cases use {@link #putMapping(Map)} instead and provide the body of
         * the PUT mapping API call.
         *
         * @param property The name of the new property.
         * @param type The mapping type.
         * @return <code>this</code> to allow for method chaining.
         */
        public Builder addProperty(String property, String type) {
            Map<String, ?> body = Map.of("properties", Map.of(property, Map.of("type", type)));
            return putMapping(body);
        }

        /**
         * Adds a change to an existing index mapping. Use {@link #addProperty(String, String)} instead for simple cases.
         *
         * @param body The complete body for this mapping change.
         * @return <code>this</code> to allow for method chaining.
         */
        public Builder putMapping(Map<String, ?> body) {
            checkVersionSet();
            this.migrations.add(new PutMappingMigration(targetVersion, body));
            return this;
        }

        /**
         * Adds or modifies dynamic index settings.
         *
         * @param settings A settings object representing the required dynamic index settings.
         * @return <code>this</code> to allow for method chaining.
         */
        public Builder dynamicSettings(Settings settings) {
            checkVersionSet();
            this.migrations.add(new DynamicIndexSettingsMigration(targetVersion, settings));
            return this;
        }

        /**
         * Builds the specified list of migrations.
         *
         * @param indexVersion The current index version.
         * @return An unmodifiable list of migrations in program order (i.e. the order in which they have been called).
         */
        public List<Migration> build(int indexVersion) {
            // ensure that the index template version is up-to-date after all migrations have been applied
            Map<String, ?> updateIndexTemplateVersion = Map.of(
                "_meta",
                Map.of("index-template-version", targetVersion, "index-version", indexVersion)
            );
            migrations.add(new PutMappingMigration(targetVersion, updateIndexTemplateVersion));
            return Collections.unmodifiableList(migrations);
        }
    }

    class PutMappingMigration implements Migration {
        private final int targetVersion;
        private final Map<String, ?> body;

        public PutMappingMigration(int targetVersion, Map<String, ?> body) {
            this.targetVersion = targetVersion;
            this.body = body;
        }

        @Override
        public int getTargetIndexTemplateVersion() {
            return targetVersion;
        }

        public void apply(String index, Consumer<PutMappingRequest> mappingConsumer, Consumer<UpdateSettingsRequest> settingsConsumer) {
            mappingConsumer.accept(new PutMappingRequest(index).source(body));
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "put mapping to target version [%d]", targetVersion);
        }
    }

    class DynamicIndexSettingsMigration implements Migration {
        private final int targetVersion;
        private final Settings settings;

        public DynamicIndexSettingsMigration(int targetVersion, Settings settings) {
            this.targetVersion = targetVersion;
            this.settings = settings;
        }

        @Override
        public int getTargetIndexTemplateVersion() {
            return targetVersion;
        }

        public void apply(String index, Consumer<PutMappingRequest> mappingConsumer, Consumer<UpdateSettingsRequest> settingsConsumer) {
            settingsConsumer.accept(new UpdateSettingsRequest(settings, index));
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "update settings to target version [%d]", targetVersion);
        }
    }
}
