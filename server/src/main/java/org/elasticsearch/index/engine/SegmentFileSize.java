/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;

/**
 * Lazily get the file size for a segment.
 */
public class SegmentFileSize {
    public final String id;
    private final SegmentCommitInfo commit;

    public SegmentFileSize(SegmentCommitInfo commit) {
        this.id = StringHelper.idToString(commit.getId());
        this.commit = commit;
    }

    public String id() {
        return id;
    }

    public long sizeInBytes() throws IOException {
        return commit.sizeInBytes();
    }
}
