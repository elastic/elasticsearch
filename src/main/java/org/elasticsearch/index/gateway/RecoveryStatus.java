/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class RecoveryStatus {

    public static enum Stage {
        INIT,
        INDEX,
        START,
        TRANSLOG,
        DONE
    }

    private Stage stage = Stage.INIT;

    private long startTime = System.currentTimeMillis();

    private long time;

    private Index index = new Index();

    private Translog translog = new Translog();

    private Start start = new Start();

    public Stage stage() {
        return this.stage;
    }

    public RecoveryStatus updateStage(Stage stage) {
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

    public Index index() {
        return index;
    }

    public Start start() {
        return this.start;
    }

    public Translog translog() {
        return translog;
    }

    public static class Start {
        private long startTime;
        private long time;
        private long checkIndexTime;

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

        public long checkIndexTime() {
            return checkIndexTime;
        }

        public void checkIndexTime(long checkIndexTime) {
            this.checkIndexTime = checkIndexTime;
        }
    }

    public static class Translog {
        private long startTime = 0;
        private long time;
        private volatile int currentTranslogOperations = 0;

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

        public void addTranslogOperations(int count) {
            this.currentTranslogOperations += count;
        }

        public int currentTranslogOperations() {
            return this.currentTranslogOperations;
        }
    }

    public static class Index {
        private long startTime = 0;
        private long time = 0;

        private long version = -1;
        private int numberOfFiles = 0;
        private long totalSize = 0;
        private int numberOfReusedFiles = 0;
        private long reusedTotalSize = 0;
        private AtomicLong currentFilesSize = new AtomicLong();

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

        public long version() {
            return this.version;
        }

        public void files(int numberOfFiles, long totalSize, int numberOfReusedFiles, long reusedTotalSize) {
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
            this.numberOfReusedFiles = numberOfReusedFiles;
            this.reusedTotalSize = reusedTotalSize;
        }

        public int numberOfFiles() {
            return numberOfFiles;
        }

        public int numberOfRecoveredFiles() {
            return numberOfFiles - numberOfReusedFiles;
        }

        public long totalSize() {
            return this.totalSize;
        }

        public int numberOfReusedFiles() {
            return numberOfReusedFiles;
        }

        public long reusedTotalSize() {
            return this.reusedTotalSize;
        }

        public long recoveredTotalSize() {
            return totalSize - reusedTotalSize;
        }

        public void updateVersion(long version) {
            this.version = version;
        }

        public long currentFilesSize() {
            return this.currentFilesSize.get();
        }

        public void addCurrentFilesSize(long updatedSize) {
            this.currentFilesSize.addAndGet(updatedSize);
        }
    }
}
