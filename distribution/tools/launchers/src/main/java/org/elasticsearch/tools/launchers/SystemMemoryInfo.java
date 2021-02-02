/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tools.launchers;

/**
 * Determines available system memory that could be allocated for Elasticsearch, to include JVM heap and other native processes.
 * The "available system memory" is defined as the total system memory which is visible to the Elasticsearch process. For instances
 * in which Elasticsearch is running in a containerized environment (i.e. Docker) this is expected to be the limits set for the container,
 * not the host system.
 */
public interface SystemMemoryInfo {

    /**
     *
     * @return total system memory available to heap or native process allocation in bytes
     * @throws SystemMemoryInfoException if unable to determine available system memory
     */
    long availableSystemMemory() throws SystemMemoryInfoException;

    class SystemMemoryInfoException extends Exception {
        public SystemMemoryInfoException(String message) {
            super(message);
        }
    }
}
