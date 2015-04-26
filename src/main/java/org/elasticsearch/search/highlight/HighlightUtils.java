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
package org.elasticsearch.search.highlight;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;

public final class HighlightUtils {

    //U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting (postings highlighter)
    public static final char PARAGRAPH_SEPARATOR = 8233;

    private HighlightUtils() {

    }

    static List<Object> loadFieldValues(SearchContextHighlight.Field field, FieldMapper<?> mapper, SearchContext searchContext, FetchSubPhase.HitContext hitContext) throws IOException {
        //percolator needs to always load from source, thus it sets the global force source to true
        boolean forceSource = searchContext.highlight().forceSource(field);
        List<Object> textsToHighlight;
        if (!forceSource && mapper.fieldType().stored()) {
            CustomFieldsVisitor fieldVisitor = new CustomFieldsVisitor(ImmutableSet.of(mapper.names().indexName()), false);
            hitContext.reader().document(hitContext.docId(), fieldVisitor);
            textsToHighlight = fieldVisitor.fields().get(mapper.names().indexName());
            if (textsToHighlight == null) {
                // Can happen if the document doesn't have the field to highlight
                textsToHighlight = ImmutableList.of();
            }
        } else {
            SourceLookup sourceLookup = searchContext.lookup().source();
            sourceLookup.setSegmentAndDocument(hitContext.readerContext(), hitContext.docId());
            textsToHighlight = sourceLookup.extractRawValues(hitContext.getSourcePath(mapper.names().sourcePath()));
        }
        assert textsToHighlight != null;
        return textsToHighlight;
    }

    static class Encoders {
        static Encoder DEFAULT = new DefaultEncoder();
        static Encoder HTML = new SimpleHTMLEncoder();
    }
}
