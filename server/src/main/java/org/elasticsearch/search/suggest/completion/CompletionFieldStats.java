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

package org.elasticsearch.search.suggest.completion;

import com.carrotsearch.hppc.ObjectLongHashMap;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.regex.Regex;

import java.io.IOException;

public class CompletionFieldStats {

    /**
     * Returns total in-heap bytes used by all suggesters.  This method has CPU cost <code>O(numIndexedFields)</code>.
     *
     * @param fieldNamePatterns if non-null, any completion field name matching any of these patterns will break out its in-heap bytes
     * separately in the returned {@link CompletionStats}
     */
    public static CompletionStats completionStats(IndexReader indexReader, String ... fieldNamePatterns) {
        long sizeInBytes = 0;
        ObjectLongHashMap<String> completionFields = null;
        if (fieldNamePatterns != null  && fieldNamePatterns.length > 0) {
            completionFields = new ObjectLongHashMap<>(fieldNamePatterns.length);
        }
        for (LeafReaderContext atomicReaderContext : indexReader.leaves()) {
            LeafReader atomicReader = atomicReaderContext.reader();
            try {
                for (FieldInfo info : atomicReader.getFieldInfos()) {
                    Terms terms = atomicReader.terms(info.name);
                    if (terms instanceof CompletionTerms) {
                        // TODO: currently we load up the suggester for reporting its size
                        long fstSize = ((CompletionTerms) terms).suggester().ramBytesUsed();
                        if (fieldNamePatterns != null && fieldNamePatterns.length > 0 && Regex.simpleMatch(fieldNamePatterns, info.name)) {
                            completionFields.addTo(info.name, fstSize);
                        }
                        sizeInBytes += fstSize;
                    }
                }
            } catch (IOException ioe) {
                throw new ElasticsearchException(ioe);
            }
        }
        return new CompletionStats(sizeInBytes, completionFields == null ? null : new FieldMemoryStats(completionFields));
    }
}
