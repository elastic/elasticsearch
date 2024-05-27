/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters;

import org.gradle.api.provider.Property;
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;
import org.jetbrains.annotations.Nullable;

public abstract class TestClusterValueSource implements ValueSource<TestClusterInfo, TestClusterValueSource.Parameters> {

    @Nullable
    @Override
    public TestClusterInfo obtain() {
        String clusterName = getParameters().getClusterName().get();
        return getParameters().getService().get().getClusterDetails(clusterName);
    }

    interface Parameters extends ValueSourceParameters {
        Property<String> getClusterName();

        Property<TestClustersRegistry> getService();
    }
}
