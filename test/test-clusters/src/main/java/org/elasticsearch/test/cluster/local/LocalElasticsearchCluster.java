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
import org.elasticsearch.test.cluster.local.distribution.SnapshotDistributionResolver;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.nio.file.Path;

public class LocalElasticsearchCluster implements ElasticsearchCluster {
    private final LocalClusterSpec spec;
    private LocalClusterHandle handle;

    public LocalElasticsearchCluster(LocalClusterSpec spec) {
        this.spec = spec;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    handle = new LocalClusterFactory(
                        Path.of(System.getProperty("java.io.tmpdir")).resolve(description.getDisplayName()).toAbsolutePath(),
                        new LocalDistributionResolver(new SnapshotDistributionResolver())
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

    private void checkHandle() {
        if (handle == null) {
            throw new IllegalStateException("Cluster handle has not been initialized. Did you forget the @ClassRule annotation?");
        }
    }
}
