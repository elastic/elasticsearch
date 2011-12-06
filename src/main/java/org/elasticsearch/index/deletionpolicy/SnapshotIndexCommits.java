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

package org.elasticsearch.index.deletionpolicy;

import org.elasticsearch.common.lease.Releasable;

import java.util.Iterator;
import java.util.List;

/**
 * Represents a snapshot view of several commits. Provides a way to iterate over
 * them as well as a simple method to release all of them.
 *
 *
 */
public class SnapshotIndexCommits implements Iterable<SnapshotIndexCommit>, Releasable {

    private final List<SnapshotIndexCommit> commits;

    public SnapshotIndexCommits(List<SnapshotIndexCommit> commits) {
        this.commits = commits;
    }

    public int size() {
        return commits.size();
    }

    @Override
    public Iterator<SnapshotIndexCommit> iterator() {
        return commits.iterator();
    }

    public boolean release() {
        boolean result = false;
        for (SnapshotIndexCommit snapshot : commits) {
            result |= snapshot.release();
        }
        return result;
    }
}
