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
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class HighlightPhase extends AbstractComponent implements FetchSubPhase {

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
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticsearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.highlight() != null;
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticsearchException {
        Map<String, HighlightField> highlightFields = newHashMap();
        for (SearchContextHighlight.Field field : context.highlight().fields()) {
            List<String> fieldNamesToHighlight;
            if (Regex.isSimpleMatchPattern(field.field())) {
                DocumentMapper documentMapper = context.mapperService().documentMapper(hitContext.hit().type());
                fieldNamesToHighlight = documentMapper.mappers().simpleMatchToFullName(field.field());
            } else {
                fieldNamesToHighlight = ImmutableList.of(field.field());
            }

            if (context.highlight().forceSource(field)) {
                SourceFieldMapper sourceFieldMapper = context.mapperService().documentMapper(hitContext.hit().type()).sourceMapper();
                if (!sourceFieldMapper.enabled()) {
                    throw new ElasticsearchIllegalArgumentException("source is forced for fields " +  fieldNamesToHighlight + " but type [" + hitContext.hit().type() + "] has disabled _source");
                }
            }

            for (String fieldName : fieldNamesToHighlight) {
                FieldMapper<?> fieldMapper = getMapperForField(fieldName, context, hitContext);
                if (fieldMapper == null) {
                    continue;
                }

                String highlighterType = field.fieldOptions().highlighterType();
                if (highlighterType == null) {
                    boolean useFastVectorHighlighter = fieldMapper.fieldType().storeTermVectors() && fieldMapper.fieldType().storeTermVectorOffsets() && fieldMapper.fieldType().storeTermVectorPositions();
                    if (useFastVectorHighlighter) {
                        highlighterType = "fvh";
                    } else if (fieldMapper.fieldType().indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
                        highlighterType = "postings";
                    } else {
                        highlighterType = "plain";
                    }
                }

                Highlighter highlighter = highlighters.get(highlighterType);
                if (highlighter == null) {
                    throw new ElasticsearchIllegalArgumentException("unknown highlighter type [" + highlighterType + "] for the field [" + fieldName + "]");
                }

                HighlighterContext.HighlightQuery highlightQuery;
                if (field.fieldOptions().highlightQuery() == null) {
                    highlightQuery = new HighlighterContext.HighlightQuery(context.parsedQuery().query(), context.query(), context.queryRewritten());
                } else {
                    highlightQuery = new HighlighterContext.HighlightQuery(field.fieldOptions().highlightQuery(), field.fieldOptions().highlightQuery(), false);
                }
                HighlighterContext highlighterContext = new HighlighterContext(fieldName, field, fieldMapper, context, hitContext, highlightQuery);
                HighlightField highlightField = highlighter.highlight(highlighterContext);
                if (highlightField != null) {
                    highlightFields.put(highlightField.name(), highlightField);
                }
            }
        }

        hitContext.hit().highlightFields(highlightFields);
    }

    private FieldMapper<?> getMapperForField(String fieldName, SearchContext searchContext, HitContext hitContext) {
        DocumentMapper documentMapper = searchContext.mapperService().documentMapper(hitContext.hit().type());
        // TODO: no need to lookup the doc mapper with unambiguous field names? just look at the mapper service
        return documentMapper.mappers().smartNameFieldMapper(fieldName);
    }
}
