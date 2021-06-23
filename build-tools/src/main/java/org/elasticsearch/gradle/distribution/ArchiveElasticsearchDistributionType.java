/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.distribution;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.ElasticsearchDistributionType;
import org.elasticsearch.gradle.Version;

public class ArchiveElasticsearchDistributionType implements ElasticsearchDistributionType {

    ArchiveElasticsearchDistributionType() {}

    @Override
    public String getName() {
        return "archive";
    }

    @Override
    public String getExtension(ElasticsearchDistribution.Platform platform) {
        return platform == ElasticsearchDistribution.Platform.WINDOWS ? "zip" : "tar.gz";
    }

    @Override
    public String getClassifier(ElasticsearchDistribution.Platform platform, Version version) {
        return version.onOrAfter("7.0.0") ? ":" + platform + "-" + Architecture.current().classifier : "";
    }

    @Override
    public boolean shouldExtract() {
        return true;
    }

}
