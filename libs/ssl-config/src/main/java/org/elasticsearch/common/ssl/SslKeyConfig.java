/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.X509ExtendedKeyManager;
import java.nio.file.Path;
import java.util.Collection;

/**
 * An interface for building a key manager at runtime.
 * The method for constructing the key manager is implementation dependent.
 */
public interface SslKeyConfig {

    /**
     * @return A collection of files that are read by this config object.
     * The {@link #createKeyManager()} method will read these files dynamically, so the behaviour of this key config may change whenever
     * any of these files are modified.
     */
    Collection<Path> getDependentFiles();

    /**
     * @return A new {@link X509ExtendedKeyManager}.
     * @throws SslConfigException if there is a problem configuring the key manager.
     */
    X509ExtendedKeyManager createKeyManager();

}

