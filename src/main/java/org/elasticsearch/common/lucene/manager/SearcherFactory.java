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
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;

/**
 * Factory class used by {@link org.apache.lucene.search.SearcherManager} and {@link org.apache.lucene.search.NRTManager} to
 * create new IndexSearchers. The default implementation just creates
 * an IndexSearcher with no custom behavior:
 * <p/>
 * <pre class="prettyprint">
 * public IndexSearcher newSearcher(IndexReader r) throws IOException {
 * return new IndexSearcher(r);
 * }
 * </pre>
 * <p/>
 * You can pass your own factory instead if you want custom behavior, such as:
 * <ul>
 * <li>Setting a custom scoring model: {@link org.apache.lucene.search.IndexSearcher#setSimilarity(org.apache.lucene.search.Similarity)}
 * <li>Parallel per-segment search: {@link org.apache.lucene.search.IndexSearcher#IndexSearcher(org.apache.lucene.index.IndexReader, java.util.concurrent.ExecutorService)}
 * <li>Return custom subclasses of IndexSearcher (for example that implement distributed scoring)
 * <li>Run queries to warm your IndexSearcher before it is used. Note: when using near-realtime search
 * you may want to also {@link org.apache.lucene.index.IndexWriterConfig#setMergedSegmentWarmer(org.apache.lucene.index.IndexWriter.IndexReaderWarmer)} to warm
 * newly merged segments in the background, outside of the reopen path.
 * </ul>
 *
 * @lucene.experimental
 */
// LUCENE MONITOR: 3.6 Remove this once 3.6 is out and use it
public class SearcherFactory {
    /**
     * Returns a new IndexSearcher over the given reader.
     */
    public IndexSearcher newSearcher(IndexReader reader) throws IOException {
        return new IndexSearcher(reader);
    }
}
