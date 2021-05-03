/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

public interface DistributionDependency {
    static DistributionDependency of(String dependencyNotation) {
        return new StringBasedDistributionDependency(dependencyNotation);
    }

    Object getDefaultNotation();

    Object getExtractedNotation();

    class StringBasedDistributionDependency implements DistributionDependency {
        private final String notation;

        public StringBasedDistributionDependency(String notation) {
            this.notation = notation;
        }

        @Override
        public Object getDefaultNotation() {
            return notation;
        }

        @Override
        public Object getExtractedNotation() {
            return notation;
        }
    }
}
