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

package org.elasticsearch.index.shard;

/**
 *
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

    private long time;

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

    public long time() {
        return this.time;
    }

    public void time(long time) {
        this.time = time;
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
        private long time;

        private int numberOfFiles;
        private long totalSize;

        public long startTime() {
            return this.startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public long time() {
            return this.time;
        }

        public void time(long time) {
            this.time = time;
        }

        public void files(int numberOfFiles, long totalSize) {
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
        }

        public int numberOfFiles() {
            return numberOfFiles;
        }

        public long totalSize() {
            return totalSize;
        }
    }

    public static class Translog {
        private long startTime;
        private long time;
        private int expectedNumberOfOperations;

        public long startTime() {
            return this.startTime;
        }

        public void startTime(long startTime) {
            this.startTime = startTime;
        }

        public long time() {
            return this.time;
        }

        public void time(long time) {
            this.time = time;
        }

        public int expectedNumberOfOperations() {
            return expectedNumberOfOperations;
        }

        public void expectedNumberOfOperations(int expectedNumberOfOperations) {
            this.expectedNumberOfOperations = expectedNumberOfOperations;
        }
    }
}
