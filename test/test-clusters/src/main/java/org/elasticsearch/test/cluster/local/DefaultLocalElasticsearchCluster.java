/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class DefaultLocalElasticsearchCluster<S extends LocalClusterSpec, H extends LocalClusterHandle> implements ElasticsearchCluster {
    private final Supplier<S> specProvider;
    private final LocalClusterFactory<S, H> clusterFactory;
    private H handle;

    public DefaultLocalElasticsearchCluster(Supplier<S> specProvider, LocalClusterFactory<S, H> clusterFactory) {
        this.specProvider = specProvider;
        this.clusterFactory = clusterFactory;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                S spec = specProvider.get();
                try {
                    if (spec.isShared() == false || handle == null) {
                        if (spec.isShared()) {
                            maybeCheckThreadLeakFilters(description);
                        }
                        handle = clusterFactory.create(spec);
                        handle.start();
                    }
                    base.evaluate();
                } finally {
                    if (spec.isShared() == false) {
                        close();
                    }
                }
            }
        };
    }

    @Override
    public int getNumNodes() {
        return handle.getNumNodes();
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
    public void stopNode(int index, boolean forcibly) {
        checkHandle();
        handle.stopNode(index, forcibly);
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
    public String getName(int index) {
        checkHandle();
        return handle.getName(index);
    }

    @Override
    public long getPid(int index) {
        checkHandle();
        return handle.getPid(index);
    }

    @Override
    public String getTransportEndpoints() {
        checkHandle();
        return handle.getTransportEndpoints();
    }

    @Override
    public List<String> getAvailableTransportEndpoints() {
        checkHandle();
        return handle.getAvailableTransportEndpoints();
    }

    @Override
    public String getTransportEndpoint(int index) {
        checkHandle();
        return handle.getTransportEndpoint(index);
    }

    @Override
    public String getRemoteClusterServerEndpoints() {
        checkHandle();
        return handle.getRemoteClusterServerEndpoints();
    }

    @Override
    public String getRemoteClusterServerEndpoint(int index) {
        checkHandle();
        return handle.getRemoteClusterServerEndpoint(index);
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

    @Override
    public InputStream getNodeLog(int index, LogType logType) {
        checkHandle();
        return handle.getNodeLog(index, logType);
    }

    @Override
    public void updateStoredSecureSettings() {
        checkHandle();
        handle.updateStoredSecureSettings();
    }

    protected H getHandle() {
        return handle;
    }

    protected void checkHandle() {
        if (handle == null) {
            throw new IllegalStateException("Cluster handle has not been initialized. Did you forget the @ClassRule annotation?");
        }
    }

    /**
     * Check for {@code TestClustersThreadFilter} if necessary. We use reflection here to avoid a dependency on randomized runner.
     */
    @SuppressWarnings("unchecked")
    private void maybeCheckThreadLeakFilters(Description description) {
        try {
            Class<? extends Annotation> threadLeakFiltersClass = (Class<? extends Annotation>) Class.forName(
                "com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters"
            );
            Annotation[] annotations = description.getTestClass().getAnnotationsByType(threadLeakFiltersClass);
            for (Annotation annotation : annotations) {
                try {
                    Class<?>[] classes = (Class<?>[]) annotation.getClass().getMethod("filters").invoke(annotation);
                    if (Arrays.stream(classes).noneMatch(c -> c.getName().equals("org.elasticsearch.test.TestClustersThreadFilter"))) {
                        throw new IllegalStateException(
                            "TestClustersThreadFilter is required when using shared clusters. Annotate your test with the following:\n\n"
                                + "    @ThreadLeakFilters(filters = TestClustersThreadFilter.class)\n"
                        );
                    }
                } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                    throw new RuntimeException("Unable to inspect filters on " + annotation, e);
                }
            }
        } catch (ClassNotFoundException e) {
            // If randomized runner isn't on the classpath then we don't care
        }
    }
}
