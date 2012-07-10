/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch;

import com.google.common.collect.Maps;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public interface FetchSubPhase {

    public static class HitContext {
        private InternalSearchHit hit;
        private IndexReader topLevelReader;
        private int topLevelDocId;
        private IndexReader reader;
        private int docId;
        private Document doc;
        private Map<String, Object> cache;
        private byte[] source;
        private Map<String, Object> sourceAsMap;

        public HitContext(byte[] source) {
            this.source = source;
            this.sourceAsMap = null;
        }

        public void reset(InternalSearchHit hit, IndexReader reader, int docId, IndexReader topLevelReader, int topLevelDocId, Document doc) {
            this.hit = hit;
            this.reader = reader;
            this.docId = docId;
            this.topLevelReader = topLevelReader;
            this.topLevelDocId = topLevelDocId;
            this.doc = doc;
        }

        public InternalSearchHit hit() {
            return hit;
        }

        public IndexReader reader() {
            return reader;
        }

        public int docId() {
            return docId;
        }

        public IndexReader topLevelReader() {
            return topLevelReader;
        }

        public int topLevelDocId() {
            return topLevelDocId;
        }

        public Document doc() {
            return doc;
        }

        public Map<String, Object> cache() {
            if (cache == null) {
                cache = Maps.newHashMap();
            }
            return cache;
        }

        public byte[] source() {
            if (source != null) {
                return source;
            }
            if (sourceAsMap != null) {
                CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
                try {
                    BytesStreamOutput streamOutput = cachedEntry.bytes();
                    // TODO: Is there a better way to figure out Content Type?
                    XContentBuilder builder = XContentFactory.jsonBuilder(streamOutput).map(sourceAsMap);
                    builder.close();
                    source = streamOutput.bytes().copyBytesArray().toBytes();
                    return source;
                } catch (IOException ex) {
                    throw new ElasticSearchException("Cannot serialize map " + sourceAsMap, ex);
                } finally {
                    CachedStreamOutput.pushEntry(cachedEntry);
                }
            }
            return null;
        }

        public Map<String, Object> sourceAsMap() {
            if (sourceAsMap != null) {
                return sourceAsMap;
            }
            if (source != null) {
                sourceAsMap = SourceLookup.sourceAsMap(source, 0, source.length);
                return sourceAsMap;
            }
            return null;
        }

        public void source(byte[] source) {
            this.source = source;
            this.sourceAsMap = null;
        }

        public void sourceAsMap(Map<String, Object>  sourceAsMap) {
            this.source = null;
            this.sourceAsMap = sourceAsMap;
        }
    }

    Map<String, ? extends SearchParseElement> parseElements();

    boolean hitExecutionNeeded(SearchContext context);

    /**
     * Executes the hit level phase, with a reader and doc id (note, its a low level reader, and the matching doc).
     */
    void hitExecute(SearchContext context, HitContext hitContext) throws ElasticSearchException;

    boolean hitsExecutionNeeded(SearchContext context);

    void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticSearchException;
}
