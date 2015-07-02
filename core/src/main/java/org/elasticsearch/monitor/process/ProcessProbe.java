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

package org.elasticsearch.monitor.process;

import org.elasticsearch.monitor.probe.Probe;

public interface ProcessProbe extends Probe {

    /**
     * Returns the process identifier (PID)
     */
    Long pid();

    /**
     * Returns the maximum number of file descriptors
     */
    Long maxFileDescriptor();

    /**
     * Returns the number of open file descriptors
     */
    Long openFileDescriptor();

    /**
     * Returns the process CPU usage in percent
     */
    Short processCpuLoad();

    /**
     * Returns the CPU time (in milliseconds) used by the process
     */
    Long processCpuTime();

    /**
     * Returns the CPU time (in milliseconds) taken by all applications threads has executed in system/kernel mode
     */
    Long processSystemTime();

    /**
     * Returns the CPU time (in milliseconds) that all applications threads has executed in user mode
     */
    Long processUserTime();

    /**
     * Returns the amount (in bytes) of virtual memory that is guaranteed to be available to the running process in bytes
     */
    Long totalVirtualMemorySize();

    /**
     * Returns the amount (in bytes) of the resident memory for the process
     */
    Long residentMemorySize();

    /**
     * Returns the amount (in bytes) of shared memory for the process
     */
    Long sharedMemorySize();
}
