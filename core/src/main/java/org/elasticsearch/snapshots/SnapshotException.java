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

package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.SnapshotName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Generic snapshot exception
 */
public class SnapshotException extends ElasticsearchException {
    private final SnapshotName snapshot;

    public SnapshotException(SnapshotName snapshot, String msg) {
        this(snapshot, msg, null);
    }

    public SnapshotException(SnapshotName snapshot, String msg, Throwable cause) {
        super("[" + (snapshot == null ? "_na" : snapshot) + "] " + msg, cause);
        this.snapshot = snapshot;
    }

    public SnapshotException(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            snapshot = SnapshotName.readSnapshotName(in);
        } else {
            snapshot = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (snapshot != null) {
            out.writeBoolean(true);
            snapshot.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public SnapshotName snapshot() {
        return snapshot;
    }
}
