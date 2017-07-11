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

package org.elasticsearch.index.translog;

import java.io.IOException;
import java.util.Arrays;

/**
 * A snapshot composed out of multiple snapshots
 */
final class MultiSnapshot implements Translog.Snapshot {

    private final Translog.Snapshot[] translogs;
    private final int totalOperations;
    private int index;

    /**
     * Creates a new point in time snapshot of the given snapshots. Those snapshots are always iterated in-order.
     */
    MultiSnapshot(Translog.Snapshot[] translogs) {
        this.translogs = translogs;
        totalOperations = Arrays.stream(translogs).mapToInt(Translog.Snapshot::totalOperations).sum();
        index = 0;
    }


    @Override
    public int totalOperations() {
        return totalOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        for (; index < translogs.length; index++) {
            final Translog.Snapshot current = translogs[index];
            Translog.Operation op = current.next();
            if (op != null) { // if we are null we move to the next snapshot
                return op;
            }
        }
        return null;
    }
}
