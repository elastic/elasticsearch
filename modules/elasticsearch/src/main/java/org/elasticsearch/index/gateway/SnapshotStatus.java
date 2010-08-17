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

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * @author kimchy (shay.banon)
 */
public class SnapshotStatus {

    public static enum Stage {
        NONE,
        INDEX,
        TRANSLOG,
        FINALIZE,
        DONE,
        FAILURE
    }

    private Stage stage = Stage.NONE;

    private long startTime;

    private long took;

    private Index index = new Index();

    private Translog translog = new Translog();

    private Throwable failure;

    public Stage stage() {
        return this.stage;
    }

    public SnapshotStatus updateStage(Stage stage) {
        this.stage = stage;
        return this;
    }

    public long startTime() {
        return this.startTime;
    }

    public void startTime(long startTime) {
        this.startTime = startTime;
    }

    public TimeValue took() {
        return new TimeValue(this.took);
    }

    public void took(long took) {
        this.took = took;
    }

    public void failed(Throwable failure) {
        this.failure = failure;
    }

    public Index index() {
        return index;
    }

    public Translog translog() {
        return translog;
    }

    public static class Index {
        private long startTime;
        private long took;

        private int numberOfFiles;
        private long totalSize = -1;

        public long startTime() {
            return this.startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public TimeValue took() {
            return new TimeValue(this.took);
        }

        public void took(long took) {
            this.took = took;
        }

        public void files(int numberOfFiles, long totalSize) {
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
        }

        public int numberOfFiles() {
            return numberOfFiles;
        }

        public ByteSizeValue totalSize() {
            return new ByteSizeValue(totalSize);
        }
    }

    public static class Translog {
        private volatile int currentTranslogOperations;

        private long startTime = -1;
        private long took;

        public long startTime() {
            return this.startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public TimeValue took() {
            return new TimeValue(this.took);
        }

        public void took(long took) {
            this.took = took;
        }

        public void addTranslogOperations(long count) {
            this.currentTranslogOperations += count;
        }

        public long currentTranslogOperations() {
            return this.currentTranslogOperations;
        }
    }
}
