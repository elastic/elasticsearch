/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.logsdb.datageneration.datasource.DataSource;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.logsdb.datageneration.fields.PredefinedField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Allows configuring behavior of data generation components.
 * @param dataSource source of generated data
 * @param maxFieldCountPerLevel maximum number of fields that an individual object in mapping has.
 *                              Applies to subobjects.
 * @param maxObjectDepth maximum depth of nested objects
 * @param nestedFieldsLimit how many total nested fields can be present in a produced mapping
 * @param fullyDynamicMapping if the mapping is fully dynamic, meaning none of the fields are mapped (essentially mapping is empty)
 * @param predefinedFields predefined fields that must be present in mapping and documents. Only top level fields are supported.
 */
public record DataGeneratorSpecification(
    DataSource dataSource,
    int maxFieldCountPerLevel,
    int maxObjectDepth,
    int nestedFieldsLimit,
    boolean fullyDynamicMapping,
    List<PredefinedField> predefinedFields
) {

    public static Builder builder() {
        return new Builder();
    }

    public static DataGeneratorSpecification buildDefault() {
        return builder().build();
    }

    public static class Builder {
        private List<DataSourceHandler> dataSourceHandlers;
        private int maxFieldCountPerLevel;
        private int maxObjectDepth;
        private int nestedFieldsLimit;
        private boolean fullyDynamicMapping;
        private List<PredefinedField> predefinedFields;

        public Builder() {
            this.dataSourceHandlers = new ArrayList<>();
            // Simply sufficiently big numbers to get some permutations
            this.maxFieldCountPerLevel = 50;
            this.maxObjectDepth = 2;
            // Default value of index.mapping.nested_fields.limit
            this.nestedFieldsLimit = 50;
            fullyDynamicMapping = false;
            this.predefinedFields = new ArrayList<>();
        }

        public Builder withDataSourceHandlers(Collection<DataSourceHandler> handlers) {
            this.dataSourceHandlers.addAll(handlers);
            return this;
        }

        public Builder withMaxFieldCountPerLevel(int maxFieldCountPerLevel) {
            this.maxFieldCountPerLevel = maxFieldCountPerLevel;
            return this;
        }

        public Builder withMaxObjectDepth(int maxObjectDepth) {
            this.maxObjectDepth = maxObjectDepth;
            return this;
        }

        public Builder withNestedFieldsLimit(int nestedFieldsLimit) {
            this.nestedFieldsLimit = nestedFieldsLimit;
            return this;
        }

        public Builder withFullyDynamicMapping(boolean fullyDynamicMapping) {
            this.fullyDynamicMapping = fullyDynamicMapping;
            return this;
        }

        public Builder withPredefinedFields(List<PredefinedField> predefinedFields) {
            this.predefinedFields = predefinedFields;
            return this;
        }

        public DataGeneratorSpecification build() {
            return new DataGeneratorSpecification(
                new DataSource(dataSourceHandlers),
                maxFieldCountPerLevel,
                maxObjectDepth,
                nestedFieldsLimit,
                fullyDynamicMapping,
                predefinedFields
            );
        }
    }
}
