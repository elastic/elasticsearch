/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

public interface ElasticsearchDistributionType {
    boolean shouldExtract();

    boolean isDockerBased();

    String getName();

    default String getExtension(ElasticsearchDistribution.Platform platform) {
        return getName();
    }

    default String getClassifier(ElasticsearchDistribution.Platform platform, Version version) {
        return ":" + Architecture.current().classifier;
    }
}
