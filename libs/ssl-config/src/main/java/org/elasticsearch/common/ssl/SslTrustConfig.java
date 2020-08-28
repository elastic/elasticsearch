/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Path;
import java.util.Collection;

/**
 * An interface for building a trust manager at runtime.
 * The method for constructing the trust manager is implementation dependent.
 */
public interface SslTrustConfig {

    /**
     * @return A collection of files that are read by this config object.
     * The {@link #createTrustManager()} method will read these files dynamically, so the behaviour of this trust config may change if
     * any of these files are modified.
     */
    Collection<Path> getDependentFiles();

    /**
     * @return A new {@link X509ExtendedTrustManager}.
     * @throws SslConfigException if there is a problem configuring the trust manager.
     */
    X509ExtendedTrustManager createTrustManager();

}

