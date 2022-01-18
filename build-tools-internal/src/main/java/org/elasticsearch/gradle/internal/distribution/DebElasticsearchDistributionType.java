/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.ElasticsearchDistributionType;
import org.elasticsearch.gradle.Version;

public class DebElasticsearchDistributionType implements ElasticsearchDistributionType {

    DebElasticsearchDistributionType() {}

    @Override
    public String getName() {
        return "deb";
    }

    @Override
    public String getClassifier(ElasticsearchDistribution.Platform platform, Version version) {
        return ":amd64";
    }

}
