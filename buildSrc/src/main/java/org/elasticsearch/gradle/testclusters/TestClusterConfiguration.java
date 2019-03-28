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
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.Distribution;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.function.Supplier;

public interface TestClusterConfiguration {
    void setVersion(String version);

    void setDistribution(Distribution distribution);

    void plugin(URI plugin);

    void plugin(File plugin);

    void keystore(String key, String value);

    void keystore(String key, Supplier<CharSequence> valueSupplier);

    void setting(String key, String value);

    void setting(String key, Supplier<CharSequence> valueSupplier);

    void systemProperty(String key, String value);

    void systemProperty(String key, Supplier<CharSequence> valueSupplier);

    void environment(String key, String value);

    void environment(String key, Supplier<CharSequence> valueSupplier);

    void freeze();

    void setJavaHome(File javaHome);

    void start();

    String getHttpSocketURI();

    String getTransportPortURI();

    List<String> getAllHttpSocketURI();

    List<String> getAllTransportPortURI();

    void stop(boolean tailLogs);
}
