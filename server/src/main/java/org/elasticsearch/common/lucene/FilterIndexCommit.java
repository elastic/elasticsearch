/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public abstract class FilterIndexCommit extends IndexCommit {
    protected final IndexCommit in;

    public FilterIndexCommit(IndexCommit in) {
        this.in = in;
    }

    public IndexCommit getIndexCommit() {
        return in;
    }

    @Override
    public String getSegmentsFileName() {
        return in.getSegmentsFileName();
    }

    @Override
    public Collection<String> getFileNames() throws IOException {
        return in.getFileNames();
    }

    @Override
    public Directory getDirectory() {
        return in.getDirectory();
    }

    @Override
    public void delete() {
        in.delete();
    }

    @Override
    public boolean isDeleted() {
        return in.isDeleted();
    }

    @Override
    public int getSegmentCount() {
        return in.getSegmentCount();
    }

    @Override
    public long getGeneration() {
        return in.getGeneration();
    }

    @Override
    public Map<String, String> getUserData() throws IOException {
        return in.getUserData();
    }

    @Override
    public String toString() {
        return "FilterIndexCommit{" + "in=" + in + '}';
    }
}
