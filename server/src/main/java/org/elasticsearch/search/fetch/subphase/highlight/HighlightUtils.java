/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchSubPhase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singleton;

public final class HighlightUtils {

    //U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting (unified highlighter)
    public static final char PARAGRAPH_SEPARATOR = 8233;
    public static final char NULL_SEPARATOR = '\u0000';

    private HighlightUtils() {

    }

    /**
     * Load field values for highlighting.
     */
    public static List<Object> loadFieldValues(MappedFieldType fieldType,
                                               SearchExecutionContext searchContext,
                                               FetchSubPhase.HitContext hitContext,
                                               boolean forceSource) throws IOException {
        if (forceSource == false && fieldType.isStored()) {
            CustomFieldsVisitor fieldVisitor = new CustomFieldsVisitor(singleton(fieldType.name()), false);
            hitContext.reader().document(hitContext.docId(), fieldVisitor);
            List<Object> textsToHighlight = fieldVisitor.fields().get(fieldType.name());
            return Objects.requireNonNullElse(textsToHighlight, Collections.emptyList());
        }
        ValueFetcher fetcher = fieldType.valueFetcher(searchContext, null);
        fetcher.setNextReader(hitContext.readerContext());
        return fetcher.fetchValues(hitContext.sourceLookup());
    }

    public static class Encoders {
        public static final Encoder DEFAULT = new DefaultEncoder();
        public static final Encoder HTML = new SimpleHTMLEncoder();
    }

}
