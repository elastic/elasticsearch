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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HighlightPhase implements FetchSubPhase {
    private final Map<String, Highlighter> highlighters;

    public HighlightPhase(Map<String, Highlighter> highlighters) {
        this.highlighters = highlighters;
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.highlight() == null) {
            return;
        }
        hitExecute(context.shardTarget(), context.getQueryShardContext(), context.parsedQuery().query(), context.highlight(), hitContext);
    }

    public void hitExecute(SearchShardTarget shardTarget,
                           QueryShardContext context,
                           Query query,
                           SearchContextHighlight highlight,
                           HitContext hitContext) {
        Map<String, HighlightField> highlightFields = new HashMap<>();
        for (SearchContextHighlight.Field field : highlight.fields()) {
            Collection<String> fieldNamesToHighlight;
            if (Regex.isSimpleMatchPattern(field.field())) {
                fieldNamesToHighlight = context.getMapperService().simpleMatchToFullName(field.field());
            } else {
                fieldNamesToHighlight = Collections.singletonList(field.field());
            }

            if (highlight.forceSource(field)) {
                SourceFieldMapper sourceFieldMapper = context.getMapperService().documentMapper().sourceMapper();
                if (sourceFieldMapper.enabled() == false) {
                    throw new IllegalArgumentException("source is forced for fields " +  fieldNamesToHighlight
                        + " but _source is disabled");
                }
            }

            boolean fieldNameContainsWildcards = field.field().contains("*");
            for (String fieldName : fieldNamesToHighlight) {
                MappedFieldType fieldType = context.getMapperService().fieldType(fieldName);
                if (fieldType == null) {
                    continue;
                }

                // We should prevent highlighting if a field is anything but a text or keyword field.
                // However, someone might implement a custom field type that has text and still want to
                // highlight on that. We cannot know in advance if the highlighter will be able to
                // highlight such a field and so we do the following:
                // If the field is only highlighted because the field matches a wildcard we assume
                // it was a mistake and do not process it.
                // If the field was explicitly given we assume that whoever issued the query knew
                // what they were doing and try to highlight anyway.
                if (fieldNameContainsWildcards) {
                    if (fieldType.typeName().equals(TextFieldMapper.CONTENT_TYPE) == false &&
                        fieldType.typeName().equals(KeywordFieldMapper.CONTENT_TYPE) == false) {
                        continue;
                    }
                }
                String highlighterType = field.fieldOptions().highlighterType();
                if (highlighterType == null) {
                    highlighterType = "unified";
                }
                Highlighter highlighter = highlighters.get(highlighterType);
                if (highlighter == null) {
                    throw new IllegalArgumentException("unknown highlighter type [" + highlighterType
                        + "] for the field [" + fieldName + "]");
                }

                Query highlightQuery = field.fieldOptions().highlightQuery();
                if (highlightQuery == null) {
                    highlightQuery = query;
                }
                HighlighterContext highlighterContext = new HighlighterContext(fieldType.name(),
                    field, fieldType, shardTarget, context, highlight, hitContext, highlightQuery);

                if ((highlighter.canHighlight(fieldType) == false) && fieldNameContainsWildcards) {
                    // if several fieldnames matched the wildcard then we want to skip those that we cannot highlight
                    continue;
                }
                HighlightField highlightField = highlighter.highlight(highlighterContext);
                if (highlightField != null) {
                    // Note that we make sure to use the original field name in the response. This is because the
                    // original field could be an alias, and highlighter implementations may instead reference the
                    // concrete field it points to.
                    highlightFields.put(fieldName,
                        new HighlightField(fieldName, highlightField.fragments()));
                }
            }
        }
        hitContext.hit().highlightFields(highlightFields);
    }
}
