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

package org.elasticsearch.index.deletionpolicy;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.IndexCommitDelegate;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A snapshot index commit point. While this is held and {@link #close()}
 * was not called, no files will be deleted that relates to this commit point
 * ({@link #getFileNames()}).
 *
 *
 */
public class SnapshotIndexCommit extends IndexCommitDelegate implements Releasable {

    private final SnapshotDeletionPolicy deletionPolicy;

    private final String[] files;

    SnapshotIndexCommit(SnapshotDeletionPolicy deletionPolicy, IndexCommit cp) throws IOException {
        super(cp);
        this.deletionPolicy = deletionPolicy;
        ArrayList<String> tmpFiles = new ArrayList<>();
        for (String o : cp.getFileNames()) {
            tmpFiles.add(o);
        }
        files = tmpFiles.toArray(new String[tmpFiles.size()]);
    }

    public String[] getFiles() {
        return files;
    }

    /**
     * Releases the current snapshot.
     */
    @Override
    public void close() {
        deletionPolicy.close(getGeneration());
    }

    /**
     * Override the delete operation, and only actually delete it if it
     * is not held by the {@link SnapshotDeletionPolicy}.
     */
    @Override
    public void delete() {
        if (!deletionPolicy.isHeld(getGeneration())) {
            delegate.delete();
        }
    }
}
