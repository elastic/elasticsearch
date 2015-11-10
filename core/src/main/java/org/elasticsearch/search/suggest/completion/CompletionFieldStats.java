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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.regex.Regex;

import java.io.IOException;

public class CompletionFieldStats {

    public static CompletionStats completionStats(IndexReader indexReader, String ... fields) {
        long sizeInBytes = 0;
        ObjectLongHashMap<String> completionFields = null;
        if (fields != null  && fields.length > 0) {
            completionFields = new ObjectLongHashMap<>(fields.length);
        }
        for (LeafReaderContext atomicReaderContext : indexReader.leaves()) {
            LeafReader atomicReader = atomicReaderContext.reader();
            try {
                for (String fieldName : atomicReader.fields()) {
                    Terms terms = atomicReader.fields().terms(fieldName);
                    if (terms instanceof CompletionTerms) {
                        // TODO: currently we load up the suggester for reporting its size
                        long fstSize = ((CompletionTerms) terms).suggester().ramBytesUsed();
                        if (fields != null && fields.length > 0 && Regex.simpleMatch(fields, fieldName)) {
                            completionFields.addTo(fieldName, fstSize);
                        }
                        sizeInBytes += fstSize;
                    }
                }
            } catch (IOException ignored) {
                throw new ElasticsearchException(ignored);
            }
        }
        return new CompletionStats(sizeInBytes, completionFields);
    }
}
