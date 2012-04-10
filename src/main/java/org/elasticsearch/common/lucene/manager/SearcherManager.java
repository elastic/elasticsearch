package org.elasticsearch.common.lucene.manager;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import java.io.IOException;

/**
 * Utility class to safely share {@link org.apache.lucene.search.IndexSearcher} instances across multiple
 * threads, while periodically reopening. This class ensures each searcher is
 * closed only once all threads have finished using it.
 * <p/>
 * <p/>
 * Use {@link #acquire} to obtain the current searcher, and {@link #release} to
 * release it, like this:
 * <p/>
 * <pre class="prettyprint">
 * IndexSearcher s = manager.acquire();
 * try {
 * // Do searching, doc retrieval, etc. with s
 * } finally {
 * manager.release(s);
 * }
 * // Do not use s after this!
 * s = null;
 * </pre>
 * <p/>
 * <p/>
 * In addition you should periodically call {@link #maybeRefresh}. While it's
 * possible to call this just before running each query, this is discouraged
 * since it penalizes the unlucky queries that do the reopen. It's better to use
 * a separate background thread, that periodically calls maybeReopen. Finally,
 * be sure to call {@link #close} once you are done.
 *
 * @lucene.experimental
 * @see SearcherFactory
 */
// LUCENE MONITOR: 3.6 Remove this once 3.6 is out and use it
public final class SearcherManager extends ReferenceManager<IndexSearcher> {

    private final SearcherFactory searcherFactory;

    /**
     * Creates and returns a new SearcherManager from the given {@link org.apache.lucene.index.IndexWriter}.
     *
     * @param writer          the IndexWriter to open the IndexReader from.
     * @param applyAllDeletes If <code>true</code>, all buffered deletes will
     *                        be applied (made visible) in the {@link org.apache.lucene.search.IndexSearcher} / {@link org.apache.lucene.index.IndexReader}.
     *                        If <code>false</code>, the deletes may or may not be applied, but remain buffered
     *                        (in IndexWriter) so that they will be applied in the future.
     *                        Applying deletes can be costly, so if your app can tolerate deleted documents
     *                        being returned you might gain some performance by passing <code>false</code>.
     *                        See {@link org.apache.lucene.index.IndexReader#openIfChanged(org.apache.lucene.index.IndexReader, org.apache.lucene.index.IndexWriter, boolean)}.
     * @param searcherFactory An optional {@link SearcherFactory}. Pass
     *                        <code>null</code> if you don't require the searcher to be warmed
     *                        before going live or other custom behavior.
     * @throws java.io.IOException
     */
    public SearcherManager(IndexWriter writer, boolean applyAllDeletes, SearcherFactory searcherFactory) throws IOException {
        if (searcherFactory == null) {
            searcherFactory = new SearcherFactory();
        }
        this.searcherFactory = searcherFactory;
        current = getSearcher(searcherFactory, IndexReader.open(writer, applyAllDeletes));
    }

    /**
     * Creates and returns a new SearcherManager from the given {@link org.apache.lucene.store.Directory}.
     *
     * @param dir             the directory to open the DirectoryReader on.
     * @param searcherFactory An optional {@link SearcherFactory}. Pass
     *                        <code>null</code> if you don't require the searcher to be warmed
     *                        before going live or other custom behavior.
     * @throws java.io.IOException
     */
    public SearcherManager(Directory dir, SearcherFactory searcherFactory) throws IOException {
        if (searcherFactory == null) {
            searcherFactory = new SearcherFactory();
        }
        this.searcherFactory = searcherFactory;
        current = getSearcher(searcherFactory, IndexReader.open(dir));
    }

    @Override
    protected void decRef(IndexSearcher reference) throws IOException {
        reference.getIndexReader().decRef();
    }

    @Override
    protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
        final IndexReader newReader = IndexReader.openIfChanged(referenceToRefresh.getIndexReader());
        if (newReader == null) {
            return null;
        } else {
            return getSearcher(searcherFactory, newReader);
        }
    }

    @Override
    protected boolean tryIncRef(IndexSearcher reference) {
        return reference.getIndexReader().tryIncRef();
    }

    /**
     * @deprecated see {@link #maybeRefresh()}.
     */
    @Deprecated
    public boolean maybeReopen() throws IOException {
        return maybeRefresh();
    }

    /**
     * Returns <code>true</code> if no changes have occured since this searcher
     * ie. reader was opened, otherwise <code>false</code>.
     *
     * @see org.apache.lucene.index.IndexReader#isCurrent()
     */
    public boolean isSearcherCurrent() throws IOException {
        final IndexSearcher searcher = acquire();
        try {
            return searcher.getIndexReader().isCurrent();
        } finally {
            release(searcher);
        }
    }

    // NOTE: decRefs incoming reader on throwing an exception
    static IndexSearcher getSearcher(SearcherFactory searcherFactory, IndexReader reader) throws IOException {
        boolean success = false;
        final IndexSearcher searcher;
        try {
            searcher = searcherFactory.newSearcher(reader);
            if (searcher.getIndexReader() != reader) {
                throw new IllegalStateException("SearcherFactory must wrap exactly the provided reader (got " + searcher.getIndexReader() + " but expected " + reader + ")");
            }
            success = true;
        } finally {
            if (!success) {
                reader.decRef();
            }
        }
        return searcher;
    }
}
