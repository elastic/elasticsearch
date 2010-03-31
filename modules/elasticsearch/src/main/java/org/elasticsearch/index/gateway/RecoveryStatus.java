/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.gateway;

import org.elasticsearch.util.SizeValue;

/**
 * @author kimchy (Shay Banon)
 */
public class RecoveryStatus {

    private Index index;

    private Translog translog;

    public RecoveryStatus(Index index, Translog translog) {
        this.index = index;
        this.translog = translog;
    }

    public Index index() {
        return index;
    }

    public Translog translog() {
        return translog;
    }

    public static class Translog {
        private long translogId;
        private int numberOfOperations;
        private SizeValue totalSize;

        public Translog(long translogId, int numberOfOperations, SizeValue totalSize) {
            this.translogId = translogId;
            this.numberOfOperations = numberOfOperations;
            this.totalSize = totalSize;
        }

        /**
         * The translog id recovered, <tt>-1</tt> indicating no translog.
         */
        public long translogId() {
            return translogId;
        }

        public int numberOfOperations() {
            return numberOfOperations;
        }

        public SizeValue totalSize() {
            return totalSize;
        }
    }

    public static class Index {
        private long version;
        private int numberOfFiles;
        private SizeValue totalSize;

        public Index(long version, int numberOfFiles, SizeValue totalSize) {
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
        }

        public long version() {
            return this.version;
        }

        public int numberOfFiles() {
            return numberOfFiles;
        }

        public SizeValue totalSize() {
            return totalSize;
        }
    }
}
