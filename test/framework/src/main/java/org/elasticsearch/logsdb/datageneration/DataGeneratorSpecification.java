/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.logsdb.datageneration.arbitrary.Arbitrary;
import org.elasticsearch.logsdb.datageneration.arbitrary.RandomBasedArbitrary;

/**
 * Allows configuring behavior of {@link  DataGenerator}.
 * @param arbitrary provides arbitrary values used during generation
 * @param maxFieldCountPerLevel maximum number of fields that an individual object in mapping has.
 *                              Applies to subobjects.
 * @param maxObjectDepth maximum depth of nested objects
 * @param nestedFieldsLimit how many total nested fields can be present in a produced mapping
 */
public record DataGeneratorSpecification(Arbitrary arbitrary, int maxFieldCountPerLevel, int maxObjectDepth, int nestedFieldsLimit) {

    public static Builder builder() {
        return new Builder();
    }

    public static DataGeneratorSpecification buildDefault() {
        return builder().build();
    }

    public static class Builder {
        private Arbitrary arbitrary;
        private int maxFieldCountPerLevel;
        private int maxObjectDepth;
        private int nestedFieldsLimit;

        public Builder() {
            // Simply sufficiently big numbers to get some permutations
            maxFieldCountPerLevel = 50;
            maxObjectDepth = 3;
            // Default value of index.mapping.nested_fields.limit
            nestedFieldsLimit = 50;
            arbitrary = new RandomBasedArbitrary();
        }

        public Builder withArbitrary(Arbitrary arbitrary) {
            this.arbitrary = arbitrary;
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

        public DataGeneratorSpecification build() {
            return new DataGeneratorSpecification(arbitrary, maxFieldCountPerLevel, maxObjectDepth, nestedFieldsLimit);
        }
    }
}
