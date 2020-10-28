/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;

final class SingleDocDirectoryReader extends DirectoryReader {
    private final SingleDocLeafReader leafReader;

    SingleDocDirectoryReader(SingleDocLeafReader leafReader) throws IOException {
        super(leafReader.getDirectory(), new LeafReader[]{leafReader});
        this.leafReader = leafReader;
    }

    private static UnsupportedOperationException unsupported() {
        assert false : "unsupported operation";
        return new UnsupportedOperationException();
    }

    @Override
    protected DirectoryReader doOpenIfChanged() {
        throw unsupported();
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
        throw unsupported();
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
        throw unsupported();
    }

    @Override
    public long getVersion() {
        throw unsupported();
    }

    @Override
    public boolean isCurrent() {
        throw unsupported();
    }

    @Override
    public IndexCommit getIndexCommit() {
        throw unsupported();
    }

    @Override
    protected void doClose() throws IOException {
        leafReader.close();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return leafReader.getReaderCacheHelper();
    }
}
