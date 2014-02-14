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

package org.elasticsearch.cluster;

/**
 * Encapsulation class used to represent the amount of disk used on a node.
 */
public class DiskUsage {
    final String nodeId;
    final long totalBytes;
    final long freeBytes;

    public DiskUsage(String nodeId, long totalBytes, long freeBytes) {
        if ((totalBytes < freeBytes) || (totalBytes < 0)) {
            throw new IllegalStateException("Free bytes [" + freeBytes +
                    "] cannot be less than 0 or greater than total bytes [" + totalBytes + "]");
        }
        this.nodeId = nodeId;
        this.totalBytes = totalBytes;
        this.freeBytes = freeBytes;
    }

    public double getFreeDiskAsPercentage() {
        double freePct = 100.0 * ((double)freeBytes / totalBytes);
        return freePct;
    }

    public long getFreeBytes() {
        return freeBytes;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public long getUsedBytes() {
        return getTotalBytes() - getFreeBytes();
    }

    public String toString() {
        return "[" + nodeId + "] free: " + getFreeBytes() + "[" + getFreeDiskAsPercentage() + "]";
    }
}
