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
package org.elasticsearch.gradle.clusterformation;

import org.elasticsearch.gradle.Distribution;
import org.elasticsearch.gradle.Version;

import javax.annotation.Nullable;
import java.io.File;

public interface ElasticsearchConfiguration {

    /**
     * Get the name of this instance as configured by the conformation declaration.
     *
     * @return the name of this instance
     */
    String getName();

    /**
     * Configure the version of Elasticsearch to be used.
     *
     * @param version the version to use
     */
    void setVersion(Version version);

    /**
     * Convenience method allowing a string to be parsed into a version
     *
     * @param version the version to parse
     */

    default void setVersion(String version) {
        setVersion(Version.fromString(version));
    }

    Version getVersion();

    /**
     * Mandatory. Configure the distribution to be used for running Elasticsearch.
     */
    void setDistribution(Distribution distribution);

    /**
     * Get the configured distribution
     * @return The configured distribution or Null iff
     */
    Distribution getDistribution();

    /**
     * Optional. Configured the JAVA_HOME used to start Elasticsearch
     *
     * When not configured no JAVA_HOME will be passed to Elasticsearch.
     *
     * @param javaHome location of java home, must exists, cannot be null
     */
    void setJavaHome(File javaHome);

    /**
     * Get the javaHome configured
     * @return the java home configured or null if none was configured
     */
    @Nullable
    File getJavaHome();

    /**
     * Gets the URI usable for a http connection
     *
     * @return URI sting e.x "127.0.0.1:43210" or "[::1]:43210"
     */
    String getHttpSocketURI();

    /**
     * Gets the URI usable for a transport connection
     *
     * @return URI sting e.x "127.0.0.1:43210" or "[::1]:43210"
     */
    String getTransportPortURI();

    /**
     * Return the configuration directory.
     *
     * The implementation is free to return a copy rather than the original, so making changes to this configuration
     * might not cause any effect.
     *
     * @return
     */
    File getConfDir();

    /**
     * Checks that all mandatory fields are configured
     */
    default void assertValid() {
        if (getDistribution() == null) {
            throw new ClusterFormationException("Missing distribution for cluster `" + getName() + "`");
        }
        if (getVersion() == null) {
            throw new ClusterFormationException("Missing version for cluster `" + getName() + "`");
        }
    }

}
