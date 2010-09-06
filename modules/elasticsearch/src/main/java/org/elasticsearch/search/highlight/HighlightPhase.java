/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.vectorhighlight.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.*;

/**
 * @author kimchy (shay.banon)
 */
public class HighlightPhase implements SearchPhase {

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("highlight", new HighlighterParseElement());
    }

    @Override public void preProcess(SearchContext context) {
    }

    @Override public void execute(SearchContext context) throws ElasticSearchException {
        if (context.highlight() == null) {
            return;
        }

        for (SearchHit hit : context.fetchResult().hits().hits()) {
            InternalSearchHit internalHit = (InternalSearchHit) hit;

            DocumentMapper documentMapper = context.mapperService().type(internalHit.type());
            int docId = internalHit.docId();

            Map<String, HighlightField> highlightFields = newHashMap();
            for (SearchContextHighlight.Field field : context.highlight().fields()) {
                String fieldName = field.field();
                FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(field.field());
                if (mapper != null) {
                    fieldName = mapper.names().indexName();
                }

                FastVectorHighlighter highlighter = buildHighlighter(field);
                FieldQuery fieldQuery = buildFieldQuery(highlighter, context.query(), context.searcher().getIndexReader(), field);

                String[] fragments;
                try {
                    fragments = highlighter.getBestFragments(fieldQuery, context.searcher().getIndexReader(), docId, fieldName, field.fragmentCharSize(), field.numberOfFragments());
                } catch (IOException e) {
                    throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                }
                HighlightField highlightField = new HighlightField(field.field(), fragments);
                highlightFields.put(highlightField.name(), highlightField);
            }

            internalHit.highlightFields(highlightFields);
        }
    }

    private FieldQuery buildFieldQuery(FastVectorHighlighter highlighter, Query query, IndexReader indexReader, SearchContextHighlight.Field field) {
        CustomFieldQuery.reader.set(indexReader);
        CustomFieldQuery.highlightFilters.set(field.highlightFilter());
        return new CustomFieldQuery(query, highlighter);
    }

    private FastVectorHighlighter buildHighlighter(SearchContextHighlight.Field field) {
        FragListBuilder fragListBuilder;
        FragmentsBuilder fragmentsBuilder;
        if (field.numberOfFragments() == 0) {
            fragListBuilder = new SingleFragListBuilder();
            fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
            // a HACK to make highlighter do highlighting, even though its using the single frag list builder
            field.numberOfFragments(1);
        } else {
            fragListBuilder = new SimpleFragListBuilder();
            if (field.scoreOrdered()) {
                fragmentsBuilder = new ScoreOrderFragmentsBuilder(field.preTags(), field.postTags());
            } else {
                fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
            }
        }

        return new FastVectorHighlighter(true, false, fragListBuilder, fragmentsBuilder);
    }
}
