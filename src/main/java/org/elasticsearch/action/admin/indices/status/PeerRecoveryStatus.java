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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class PeerRecoveryStatus {

    public enum Stage {
        INIT((byte) 0),
        INDEX((byte) 1),
        TRANSLOG((byte) 2),
        FINALIZE((byte) 3),
        DONE((byte) 4);

        private final byte value;

        Stage(byte value) {
            this.value = value;
        }

        public byte value() {
            return value;
        }

        public static Stage fromValue(byte value) {
            if (value == 0) {
                return INIT;
            } else if (value == 1) {
                return INDEX;
            } else if (value == 2) {
                return TRANSLOG;
            } else if (value == 3) {
                return FINALIZE;
            } else if (value == 4) {
                return DONE;
            }
            throw new ElasticSearchIllegalArgumentException("No stage found for [" + value + ']');
        }
    }

    final Stage stage;

    final long startTime;

    final long time;

    final long indexSize;

    final long reusedIndexSize;

    final long recoveredIndexSize;

    final long recoveredTranslogOperations;

    public PeerRecoveryStatus(Stage stage, long startTime, long time, long indexSize, long reusedIndexSize,
                              long recoveredIndexSize, long recoveredTranslogOperations) {
        this.stage = stage;
        this.startTime = startTime;
        this.time = time;
        this.indexSize = indexSize;
        this.reusedIndexSize = reusedIndexSize;
        this.recoveredIndexSize = recoveredIndexSize;
        this.recoveredTranslogOperations = recoveredTranslogOperations;
    }

    public Stage getStage() {
        return this.stage;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public TimeValue getTime() {
        return TimeValue.timeValueMillis(time);
    }

    public ByteSizeValue getIndexSize() {
        return new ByteSizeValue(indexSize);
    }

    public ByteSizeValue getReusedIndexSize() {
        return new ByteSizeValue(reusedIndexSize);
    }

    public ByteSizeValue getExpectedRecoveredIndexSize() {
        return new ByteSizeValue(indexSize - reusedIndexSize);
    }

    /**
     * How much of the index has been recovered.
     */
    public ByteSizeValue getRecoveredIndexSize() {
        return new ByteSizeValue(recoveredIndexSize);
    }

    public int getIndexRecoveryProgress() {
        if (recoveredIndexSize == 0) {
            if (indexSize != 0 && indexSize == reusedIndexSize) {
                return 100;
            }
            return 0;
        }
        return (int) (((double) recoveredIndexSize) / getExpectedRecoveredIndexSize().bytes() * 100);
    }

    public long getRecoveredTranslogOperations() {
        return recoveredTranslogOperations;
    }
}
