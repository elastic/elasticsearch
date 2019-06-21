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

package org.elasticsearch.index.engine;

import java.io.IOException;
import java.util.function.BiConsumer;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.common.SuppressForbidden;

/**
 * Copy of {@link org.apache.lucene.index.ReaderManager} that allows
 * a consumer to be notified with the previous and new {@link DirectoryReader}
 * when the reader is refreshed.
 */
@SuppressForbidden(reason = "reference counting is required here")
class ReaderManager extends ReferenceManager<DirectoryReader> {
    protected final BiConsumer<DirectoryReader, DirectoryReader> newReaderConsumer;

    /**
     * Creates and returns a new ReaderManager from the given
     * already-opened {@link DirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader the directoryReader to use for future reopens
     * @param newReaderConsumer A consumer that is called every time a new reader is opened
     */
    ReaderManager(DirectoryReader reader, BiConsumer<DirectoryReader, DirectoryReader> newReaderConsumer) {
        this.current = reader;
        this.newReaderConsumer = newReaderConsumer;
        newReaderConsumer.accept(current, null);
    }

    @Override
    protected void decRef(DirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected DirectoryReader refreshIfNeeded(DirectoryReader referenceToRefresh) throws IOException {
        DirectoryReader reader = DirectoryReader.openIfChanged(referenceToRefresh);
        if (reader != null) {
            newReaderConsumer.accept(reader, referenceToRefresh);
        }
        return reader;
    }

    @Override
    protected boolean tryIncRef(DirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(DirectoryReader reference) {
        return reference.getRefCount();
    }
}
