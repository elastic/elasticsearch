/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;

import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Path;
import java.util.List;

/**
 * The configuration of trust material for SSL usage
 */
abstract class TrustConfig {

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the provided configuration
     * @param environment the environment to resolve files against or null in the case of running in a transport client
     */
    abstract X509ExtendedTrustManager createTrustManager(@Nullable Environment environment);

    /**
     * Returns a list of files that should be monitored for changes
     * @param environment the environment to resolve files against or null in the case of running in a transport client
     */
    abstract List<Path> filesToMonitor(@Nullable Environment environment);

    /**
     * {@inheritDoc}. Declared as abstract to force implementors to provide a custom implementation
     */
    public abstract String toString();

    /**
     * {@inheritDoc}. Declared as abstract to force implementors to provide a custom implementation
     */
    public abstract boolean equals(Object o);

    /**
     * {@inheritDoc}. Declared as abstract to force implementors to provide a custom implementation
     */
    public abstract int hashCode();
}
