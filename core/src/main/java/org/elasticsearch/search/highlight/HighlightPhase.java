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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HighlightPhase extends AbstractComponent implements FetchSubPhase {

    private static final List<String> STANDARD_HIGHLIGHTERS_BY_PRECEDENCE = Arrays.asList("fvh", "postings", "plain");

    private final Highlighters highlighters;

    @Inject
    public HighlightPhase(Settings settings, Highlighters highlighters) {
        super(settings);
        this.highlighters = highlighters;
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("highlight", new HighlighterParseElement());
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.highlight() != null;
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        Map<String, HighlightField> highlightFields = new HashMap<>();
        for (SearchContextHighlight.Field field : context.highlight().fields()) {
            Collection<String> fieldNamesToHighlight;
            if (Regex.isSimpleMatchPattern(field.field())) {
                DocumentMapper documentMapper = context.mapperService().documentMapper(hitContext.hit().type());
                fieldNamesToHighlight = documentMapper.mappers().simpleMatchToFullName(field.field());
            } else {
                fieldNamesToHighlight = Collections.singletonList(field.field());
            }

            if (context.highlight().forceSource(field)) {
                SourceFieldMapper sourceFieldMapper = context.mapperService().documentMapper(hitContext.hit().type()).sourceMapper();
                if (!sourceFieldMapper.enabled()) {
                    throw new IllegalArgumentException("source is forced for fields " +  fieldNamesToHighlight + " but type [" + hitContext.hit().type() + "] has disabled _source");
                }
            }

            boolean fieldNameContainsWildcards = field.field().contains("*");
            for (String fieldName : fieldNamesToHighlight) {
                FieldMapper fieldMapper = getMapperForField(fieldName, context, hitContext);
                if (fieldMapper == null) {
                    continue;
                }

                String highlighterType = field.fieldOptions().highlighterType();
                if (highlighterType == null) {
                    for(String highlighterCandidate : STANDARD_HIGHLIGHTERS_BY_PRECEDENCE) {
                        if (highlighters.get(highlighterCandidate).canHighlight(fieldMapper)) {
                            highlighterType = highlighterCandidate;
                            break;
                        }
                    }
                    assert highlighterType != null;
                }
                Highlighter highlighter = highlighters.get(highlighterType);
                if (highlighter == null) {
                    throw new IllegalArgumentException("unknown highlighter type [" + highlighterType + "] for the field [" + fieldName + "]");
                }

                Query highlightQuery = field.fieldOptions().highlightQuery() == null ? context.parsedQuery().query() : field.fieldOptions().highlightQuery();
                HighlighterContext highlighterContext = new HighlighterContext(fieldName, field, fieldMapper, context, hitContext, highlightQuery);

                if ((highlighter.canHighlight(fieldMapper) == false) && fieldNameContainsWildcards) {
                    // if several fieldnames matched the wildcard then we want to skip those that we cannot highlight
                    continue;
                }
                HighlightField highlightField = highlighter.highlight(highlighterContext);
                if (highlightField != null) {
                    highlightFields.put(highlightField.name(), highlightField);
                }
            }
        }
        hitContext.hit().highlightFields(highlightFields);
    }

    private FieldMapper getMapperForField(String fieldName, SearchContext searchContext, HitContext hitContext) {
        DocumentMapper documentMapper = searchContext.mapperService().documentMapper(hitContext.hit().type());
        // TODO: no need to lookup the doc mapper with unambiguous field names? just look at the mapper service
        return documentMapper.mappers().smartNameFieldMapper(fieldName);
    }
}
