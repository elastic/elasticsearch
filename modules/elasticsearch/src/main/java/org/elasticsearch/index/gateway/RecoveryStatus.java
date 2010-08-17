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

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (shay.banon)
 */
public class RecoveryStatus {

    public static enum Stage {
        NONE,
        INDEX,
        TRANSLOG,
        DONE
    }

    private Stage stage = Stage.NONE;

    private long startTime;

    private long took;

    private Index index = new Index();

    private Translog translog = new Translog();

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

    public TimeValue took() {
        return new TimeValue(this.took);
    }

    public void took(long took) {
        this.took = took;
    }

    public Index index() {
        return index;
    }

    public Translog translog() {
        return translog;
    }

    public static class Translog {
        volatile long currentTranslogOperations = 0;
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

    public static class Index {
        private long startTime = -1;
        private long took = -1;

        private long version = -1;
        private int numberOfFiles = 0;
        private long totalSize = 0;
        private int numberOfExistingFiles = 0;
        private long existingTotalSize = 0;
        private AtomicLong throttlingWaitTime = new AtomicLong();
        private AtomicLong currentFilesSize = new AtomicLong();

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

        public long version() {
            return this.version;
        }

        public void files(int numberOfFiles, long totalSize, int numberOfExistingFiles, long existingTotalSize) {
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
            this.numberOfExistingFiles = numberOfExistingFiles;
            this.existingTotalSize = existingTotalSize;
        }

        public int numberOfFiles() {
            return numberOfFiles;
        }

        public ByteSizeValue totalSize() {
            return new ByteSizeValue(totalSize);
        }

        public int numberOfExistingFiles() {
            return numberOfExistingFiles;
        }

        public ByteSizeValue existingTotalSize() {
            return new ByteSizeValue(existingTotalSize);
        }

        public void addThrottlingTime(long delta) {
            throttlingWaitTime.addAndGet(delta);
        }

        public TimeValue throttlingWaitTime() {
            return new TimeValue(throttlingWaitTime.get());
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
