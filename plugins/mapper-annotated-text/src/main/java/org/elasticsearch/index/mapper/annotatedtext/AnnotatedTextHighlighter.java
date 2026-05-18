/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.annotatedtext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedHighlighterAnalyzer;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.elasticsearch.lucene.search.uhighlight.QueryMaxAnalyzedOffset;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnnotatedTextHighlighter extends DefaultHighlighter {

    public static final String NAME = "annotated";

    // Convert the marked-up values held on-disk to plain-text versions for highlighting
    @Override
    protected List<Object> loadFieldValues(
        CustomUnifiedHighlighter highlighter,
        SearchExecutionContext searchContext,
        MappedFieldType fieldType,
        HitContext hitContext
    ) throws IOException {
        List<Object> fieldValues = super.loadFieldValues(highlighter, searchContext, fieldType, hitContext);

        List<Object> strings = new ArrayList<>(fieldValues.size());
        AnnotatedText[] annotations = new AnnotatedText[fieldValues.size()];
        for (int i = 0; i < fieldValues.size(); i++) {
            annotations[i] = AnnotatedText.parse(fieldValues.get(i).toString());
            strings.add(annotations[i].textMinusMarkup());
        }
        // Store the annotations in the formatter and analyzer
        ((AnnotatedPassageFormatter) highlighter.getFormatter()).setAnnotations(annotations);
        ((AnnotatedHighlighterAnalyzer) highlighter.getIndexAnalyzer()).setAnnotations(annotations);
        return strings;
    }

    @Override
    protected Analyzer wrapAnalyzer(Analyzer analyzer, QueryMaxAnalyzedOffset maxAnalyzedOffset) {
        return new AnnotatedHighlighterAnalyzer(super.wrapAnalyzer(analyzer, maxAnalyzedOffset));
    }

    @Override
    protected PassageFormatter getPassageFormatter(SearchHighlightContext.Field field, Encoder encoder) {
        return new AnnotatedPassageFormatter(encoder);
    }

}
