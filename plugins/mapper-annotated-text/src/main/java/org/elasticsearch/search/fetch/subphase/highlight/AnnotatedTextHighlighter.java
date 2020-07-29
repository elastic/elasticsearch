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
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnnotatedTextHighlighter extends UnifiedHighlighter {

    public static final String NAME = "annotated";

    @Override
    protected Analyzer getAnalyzer(DocumentMapper docMapper, HitContext hitContext) {
        return new AnnotatedHighlighterAnalyzer(super.getAnalyzer(docMapper, hitContext), hitContext);
    }

    // Convert the marked-up values held on-disk to plain-text versions for highlighting
    @Override
    protected List<Object> loadFieldValues(MappedFieldType fieldType,
                                           Field field,
                                           HitContext hitContext,
                                           boolean forceSource) throws IOException {
        List<Object> fieldValues = super.loadFieldValues(fieldType, field, hitContext, forceSource);
        String[] fieldValuesAsString = fieldValues.toArray(new String[fieldValues.size()]);

        AnnotatedText[] annotations = new AnnotatedText[fieldValuesAsString.length];
        for (int i = 0; i < fieldValuesAsString.length; i++) {
            annotations[i] = AnnotatedText.parse(fieldValuesAsString[i]);
        }
        // Store the annotations in the hitContext
        hitContext.cache().put(AnnotatedText.class.getName(), annotations);

        ArrayList<Object> result = new ArrayList<>(annotations.length);
        for (int i = 0; i < annotations.length; i++) {
            result.add(annotations[i].textMinusMarkup);
        }
        return result;
    }

    @Override
    protected PassageFormatter getPassageFormatter(HitContext hitContext, SearchHighlightContext.Field field, Encoder encoder) {
        // Retrieve the annotations from the hitContext
        AnnotatedText[] annotations = (AnnotatedText[]) hitContext.cache().get(AnnotatedText.class.getName());
        return new AnnotatedPassageFormatter(annotations, encoder);
    }

}
