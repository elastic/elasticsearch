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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * Encapsulation class used to represent the amount of disk used on a node.
 */
public class DiskUsage {
    final String nodeId;
    final String nodeName;
    final String path;
    final long totalBytes;
    final long freeBytes;

    /**
     * Create a new DiskUsage, if {@code totalBytes} is 0, {@link #getFreeDiskAsPercentage()}
     * will always return 100.0% free
     */
    public DiskUsage(String nodeId, String nodeName, String path, long totalBytes, long freeBytes) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.freeBytes = freeBytes;
        this.totalBytes = totalBytes;
        this.path = path;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getPath() {
        return path;
    }

    public double getFreeDiskAsPercentage() {
        // We return 100.0% in order to fail "open", in that if we have invalid
        // numbers for the total bytes, it's as if we don't know disk usage.
        if (totalBytes == 0) {
            return 100.0;
        }
        return 100.0 * ((double)freeBytes / totalBytes);
    }

    public double getUsedDiskAsPercentage() {
        return 100.0 - getFreeDiskAsPercentage();
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

    @Override
    public String toString() {
        return "[" + nodeId + "][" + nodeName + "][" + path + "] free: " + new ByteSizeValue(getFreeBytes()) +
                "[" + Strings.format1Decimals(getFreeDiskAsPercentage(), "%") + "]";
    }
}
