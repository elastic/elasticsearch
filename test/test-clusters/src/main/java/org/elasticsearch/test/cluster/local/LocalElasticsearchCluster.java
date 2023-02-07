/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.SnapshotDistributionResolver;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class LocalElasticsearchCluster implements ElasticsearchCluster {
    private final DefaultLocalClusterSpecBuilder builder;
    private LocalClusterSpec spec;
    private LocalClusterHandle handle;

    public LocalElasticsearchCluster(DefaultLocalClusterSpecBuilder builder) {
        this.builder = builder;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    spec = builder.buildClusterSpec();
                    handle = new LocalClusterFactory(
                        new LocalDistributionResolver(new SnapshotDistributionResolver(new ReleasedDistributionResolver()))
                    ).create(spec);
                    handle.start();
                    base.evaluate();
                } finally {
                    close();
                }
            }
        };
    }

    @Override
    public void start() {
        checkHandle();
        handle.start();
    }

    @Override
    public void stop(boolean forcibly) {
        checkHandle();
        handle.stop(forcibly);
    }

    @Override
    public void restart(boolean forcibly) {
        checkHandle();
        handle.restart(forcibly);
    }

    @Override
    public boolean isStarted() {
        checkHandle();
        return handle.isStarted();
    }

    @Override
    public void close() {
        checkHandle();
        handle.close();
    }

    @Override
    public String getHttpAddresses() {
        checkHandle();
        return handle.getHttpAddresses();
    }

    @Override
    public String getHttpAddress(int index) {
        checkHandle();
        return handle.getHttpAddress(index);
    }

    @Override
    public String getTransportEndpoints() {
        checkHandle();
        return handle.getTransportEndpoints();
    }

    @Override
    public String getTransportEndpoint(int index) {
        checkHandle();
        return handle.getTransportEndpoint(index);
    }

    @Override
    public void upgradeNodeToVersion(int index, Version version) {
        checkHandle();
        handle.upgradeNodeToVersion(index, version);
    }

    @Override
    public void upgradeToVersion(Version version) {
        checkHandle();
        handle.upgradeToVersion(version);
    }

    private void checkHandle() {
        if (handle == null) {
            throw new IllegalStateException("Cluster handle has not been initialized. Did you forget the @ClassRule annotation?");
        }
    }
}
