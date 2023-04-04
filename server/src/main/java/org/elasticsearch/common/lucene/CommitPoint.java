/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public final class CommitPoint extends IndexCommit {
    private final String segmentsFileName;
    private final Collection<String> files;
    private final Directory dir;
    private final long generation;
    private final Map<String, String> userData;
    private final int segmentCount;

    public CommitPoint(SegmentInfos infos, Directory dir) throws IOException {
        segmentsFileName = infos.getSegmentsFileName();
        this.dir = dir;
        userData = infos.getUserData();
        files = Collections.unmodifiableCollection(infos.files(true));
        generation = infos.getGeneration();
        segmentCount = infos.size();
    }

    @Override
    public String toString() {
        return "DirectoryReader.ReaderCommit(" + segmentsFileName + ")";
    }

    @Override
    public int getSegmentCount() {
        return segmentCount;
    }

    @Override
    public String getSegmentsFileName() {
        return segmentsFileName;
    }

    @Override
    public Collection<String> getFileNames() {
        return files;
    }

    @Override
    public Directory getDirectory() {
        return dir;
    }

    @Override
    public long getGeneration() {
        return generation;
    }

    @Override
    public boolean isDeleted() {
        return false;
    }

    @Override
    public Map<String, String> getUserData() {
        return userData;
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("This IndexCommit does not support deletions");
    }
}
