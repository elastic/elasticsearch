/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.ElasticsearchDistributionType;

public class DockerCloudElasticsearchDistributionType implements ElasticsearchDistributionType {

    DockerCloudElasticsearchDistributionType() {}

    @Override
    public String getName() {
        return "dockerCloud";
    }

    @Override
    public boolean isDocker() {
        return true;
    }
}
