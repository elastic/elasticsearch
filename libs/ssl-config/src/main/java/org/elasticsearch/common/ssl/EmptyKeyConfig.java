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

import javax.net.ssl.X509ExtendedKeyManager;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link SslKeyConfig} that does nothing (provides a null key manager)
 */
final class EmptyKeyConfig implements SslKeyConfig {

    static final EmptyKeyConfig INSTANCE = new EmptyKeyConfig();

    private EmptyKeyConfig() {
        // Enforce a single instance
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return Collections.emptyList();
    }

    @Override
    public X509ExtendedKeyManager createKeyManager() {
        return null;
    }

    @Override
    public String toString() {
        return "empty-key-config";
    }
}
