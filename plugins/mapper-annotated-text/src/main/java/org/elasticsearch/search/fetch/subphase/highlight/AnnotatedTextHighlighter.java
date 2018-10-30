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

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedHighlighterAnalyzer;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight.Field;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AnnotatedTextHighlighter extends UnifiedHighlighter {
    
    public static final String NAME = "annotated";

    AnnotatedHighlighterAnalyzer annotatedHighlighterAnalyzer = null;    
    
    @Override
    protected Analyzer getAnalyzer(DocumentMapper docMapper, MappedFieldType type) {
        annotatedHighlighterAnalyzer = new AnnotatedHighlighterAnalyzer(super.getAnalyzer(docMapper, type));
        return annotatedHighlighterAnalyzer;
    }

    // Convert the marked-up values held on-disk to plain-text versions for highlighting
    @Override
    protected List<Object> loadFieldValues(MappedFieldType fieldType, Field field, SearchContext context, HitContext hitContext)
            throws IOException {
        List<Object> fieldValues = super.loadFieldValues(fieldType, field, context, hitContext);
        String[] fieldValuesAsString = fieldValues.toArray(new String[fieldValues.size()]);
        annotatedHighlighterAnalyzer.init(fieldValuesAsString);
        return Arrays.asList((Object[]) annotatedHighlighterAnalyzer.getPlainTextValuesForHighlighter());
    }

    @Override
    protected PassageFormatter getPassageFormatter(SearchContextHighlight.Field field, Encoder encoder) {
        return new AnnotatedPassageFormatter(annotatedHighlighterAnalyzer, encoder);

    }

}
