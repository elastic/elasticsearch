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

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * A simple delegate that delegates all {@link IndexCommit} calls to a delegated
 * {@link IndexCommit}.
 *
 *
 */
public abstract class IndexCommitDelegate extends IndexCommit {

    protected final IndexCommit delegate;

    /**
     * Constructs a new {@link IndexCommit} that will delegate all calls
     * to the provided delegate.
     *
     * @param delegate The delegate
     */
    public IndexCommitDelegate(IndexCommit delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getSegmentsFileName() {
        return delegate.getSegmentsFileName();
    }

    @Override
    public Collection<String> getFileNames() throws IOException {
        return delegate.getFileNames();
    }

    @Override
    public Directory getDirectory() {
        return delegate.getDirectory();
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public boolean isDeleted() {
        return delegate.isDeleted();
    }

    @Override
    public int getSegmentCount() {
        return delegate.getSegmentCount();
    }

    @Override
    public boolean equals(Object other) {
        return delegate.equals(other);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public long getGeneration() {
        return delegate.getGeneration();
    }

    @Override
    public Map<String, String> getUserData() throws IOException {
        return delegate.getUserData();
    }
}
