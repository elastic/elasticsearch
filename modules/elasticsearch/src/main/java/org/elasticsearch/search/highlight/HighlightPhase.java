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
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.SearchHitPhase;
import org.elasticsearch.search.highlight.vectorhighlight.SourceScoreOrderFragmentsBuilder;
import org.elasticsearch.search.highlight.vectorhighlight.SourceSimpleFragmentsBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.*;

/**
 * @author kimchy (shay.banon)
 */
public class HighlightPhase implements SearchHitPhase {

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("highlight", new HighlighterParseElement());
    }

    @Override public boolean executionNeeded(SearchContext context) {
        return context.highlight() != null;
    }

    @Override public void execute(SearchContext context, InternalSearchHit hit, Uid uid, IndexReader reader, int docId) throws ElasticSearchException {
        try {
            DocumentMapper documentMapper = context.mapperService().type(hit.type());

            Map<String, HighlightField> highlightFields = newHashMap();
            for (SearchContextHighlight.Field field : context.highlight().fields()) {
                FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(field.field());
                if (mapper == null) {
                    throw new SearchException(context.shardTarget(), "No mapping found for [" + field.field() + "]");
                }

                FastVectorHighlighter highlighter = buildHighlighter(context, mapper, field);
                FieldQuery fieldQuery = buildFieldQuery(highlighter, context.query(), reader, field);

                String[] fragments;
                try {
                    // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                    int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                    fragments = highlighter.getBestFragments(fieldQuery, context.searcher().getIndexReader(), docId, mapper.names().indexName(), field.fragmentCharSize(), numberOfFragments);
                } catch (IOException e) {
                    throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                }
                HighlightField highlightField = new HighlightField(field.field(), fragments);
                highlightFields.put(highlightField.name(), highlightField);
            }

            hit.highlightFields(highlightFields);
        } finally {
            CustomFieldQuery.reader.remove();
            CustomFieldQuery.highlightFilters.remove();
        }
    }

    private FieldQuery buildFieldQuery(FastVectorHighlighter highlighter, Query query, IndexReader indexReader, SearchContextHighlight.Field field) {
        CustomFieldQuery.reader.set(indexReader);
        CustomFieldQuery.highlightFilters.set(field.highlightFilter());
        return new CustomFieldQuery(query, highlighter);
    }

    private FastVectorHighlighter buildHighlighter(SearchContext searchContext, FieldMapper fieldMapper, SearchContextHighlight.Field field) {
        FragListBuilder fragListBuilder;
        FragmentsBuilder fragmentsBuilder;
        if (field.numberOfFragments() == 0) {
            fragListBuilder = new SingleFragListBuilder();
            if (fieldMapper.stored()) {
                fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
            } else {
                fragmentsBuilder = new SourceSimpleFragmentsBuilder(fieldMapper, searchContext, field.preTags(), field.postTags());
            }
        } else {
            fragListBuilder = new SimpleFragListBuilder();
            if (field.scoreOrdered()) {
                if (fieldMapper.stored()) {
                    fragmentsBuilder = new ScoreOrderFragmentsBuilder(field.preTags(), field.postTags());
                } else {
                    fragmentsBuilder = new SourceScoreOrderFragmentsBuilder(fieldMapper, searchContext, field.preTags(), field.postTags());
                }
            } else {
                if (fieldMapper.stored()) {
                    fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
                } else {
                    fragmentsBuilder = new SourceSimpleFragmentsBuilder(fieldMapper, searchContext, field.preTags(), field.postTags());
                }
            }
        }

        return new FastVectorHighlighter(true, false, fragListBuilder, fragmentsBuilder);
    }
}
