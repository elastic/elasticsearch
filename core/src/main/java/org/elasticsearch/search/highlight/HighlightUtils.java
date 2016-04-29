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

import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singleton;

public final class HighlightUtils {

    //U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting (postings highlighter)
    public static final char PARAGRAPH_SEPARATOR = 8233;
    public static final char NULL_SEPARATOR = '\u0000';

    private HighlightUtils() {

    }

    static List<Object> loadFieldValues(SearchContextHighlight.Field field, FieldMapper mapper, SearchContext searchContext, FetchSubPhase.HitContext hitContext) throws IOException {
        //percolator needs to always load from source, thus it sets the global force source to true
        boolean forceSource = searchContext.highlight().forceSource(field);
        List<Object> textsToHighlight;
        if (!forceSource && mapper.fieldType().stored()) {
            CustomFieldsVisitor fieldVisitor = new CustomFieldsVisitor(singleton(mapper.fieldType().name()), false);
            hitContext.reader().document(hitContext.docId(), fieldVisitor);
            textsToHighlight = fieldVisitor.fields().get(mapper.fieldType().name());
            if (textsToHighlight == null) {
                // Can happen if the document doesn't have the field to highlight
                textsToHighlight = Collections.emptyList();
            }
        } else {
            SourceLookup sourceLookup = searchContext.lookup().source();
            sourceLookup.setSegmentAndDocument(hitContext.readerContext(), hitContext.docId());
            textsToHighlight = sourceLookup.extractRawValues(hitContext.getSourcePath(mapper.fieldType().name()));
        }
        assert textsToHighlight != null;
        return textsToHighlight;
    }

    static class Encoders {
        static Encoder DEFAULT = new DefaultEncoder();
        static Encoder HTML = new SimpleHTMLEncoder();
    }
}
