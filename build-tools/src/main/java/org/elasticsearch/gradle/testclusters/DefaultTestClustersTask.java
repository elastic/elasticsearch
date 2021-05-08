/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.testclusters;

import org.gradle.api.DefaultTask;

import java.util.Collection;
import java.util.HashSet;

public class DefaultTestClustersTask extends DefaultTask implements TestClustersAware {

    private Collection<ElasticsearchCluster> clusters = new HashSet<>();

    @Override
    public Collection<ElasticsearchCluster> getClusters() {
        return clusters;
    }

}
