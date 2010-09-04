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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;

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

        Map<Integer,FastVectorHighlighter> highlighterMap = newHashMap();
        Map<Integer,FieldQuery> fieldQueryMap = newHashMap();

        for (SearchHit hit : context.fetchResult().hits().hits()) {
            InternalSearchHit internalHit = (InternalSearchHit) hit;

            DocumentMapper documentMapper = context.mapperService().type(internalHit.type());
            int docId = internalHit.docId();

            Map<String, HighlightField> highlightFields = newHashMap();
            for (SearchContextHighlight.ParsedHighlightField parsedHighlightField : context.highlight().fields()) {
                String fieldName = parsedHighlightField.field();
                FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(parsedHighlightField.field());
                if (mapper != null) {
                    fieldName = mapper.names().indexName();
                }

                Tuple<Integer,FastVectorHighlighter> highlighterTuple = getHighlighter(highlighterMap, parsedHighlightField.settings());
                FastVectorHighlighter highlighter = highlighterTuple.v2();
                FieldQuery fieldQuery = getFieldQuery(highlighterTuple.v1(), fieldQueryMap, highlighter, context.query(), context.searcher().getIndexReader(), parsedHighlightField.settings());
                
                String[] fragments;
                try {
                    fragments = highlighter.getBestFragments(fieldQuery, context.searcher().getIndexReader(), docId, fieldName, parsedHighlightField.settings().fragmentCharSize(), parsedHighlightField.settings().numberOfFragments());
                } catch (IOException e) {
                    throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + parsedHighlightField.field() + "]", e);
                }
                HighlightField highlightField = new HighlightField(parsedHighlightField.field(), fragments);
                highlightFields.put(highlightField.name(), highlightField);
            }

            internalHit.highlightFields(highlightFields);
        }
    }

    private FieldQuery getFieldQuery(int key, Map<Integer,FieldQuery> fieldQueryMap, FastVectorHighlighter highlighter, Query query, IndexReader indexReader, SearchContextHighlight.ParsedHighlightSettings settings) {
        FieldQuery fq = fieldQueryMap.get(key);
        if (fq == null) {
            CustomFieldQuery.reader.set(indexReader);
            CustomFieldQuery.highlightFilters.set(settings.highlightFilter());
            fq = new CustomFieldQuery(query, highlighter);
            fieldQueryMap.put(key,fq);
        }
        return fq;
    }

    private Tuple<Integer, FastVectorHighlighter> getHighlighter(Map<Integer,FastVectorHighlighter> highlighterMap, SearchContextHighlight.ParsedHighlightSettings settings) {

        FragListBuilder fragListBuilder;
        FragmentsBuilder fragmentsBuilder;
        if (!settings.fragmentsAllowed()) {
            fragListBuilder = new SingleFragListBuilder();
            fragmentsBuilder = new SimpleFragmentsBuilder(settings.preTags(), settings.postTags());
        } else {
            fragListBuilder = new SimpleFragListBuilder();
            if (settings.scoreOrdered()) {
                fragmentsBuilder = new ScoreOrderFragmentsBuilder(settings.preTags(), settings.postTags());
            } else {
                fragmentsBuilder = new SimpleFragmentsBuilder(settings.preTags(), settings.postTags());
            }
        }

        // highlighter key is determined by tags and FragList and Fragment builder classes.
        String[] mask = Arrays.copyOf(settings.preTags(), settings.preTags().length + settings.postTags().length);
        System.arraycopy(settings.postTags(), 0, mask, settings.preTags().length, settings.postTags().length);
        int key = (Arrays.toString(mask)+fragListBuilder.getClass().getSimpleName()+fragmentsBuilder.getClass().getSimpleName()).hashCode();

        FastVectorHighlighter highlighter = highlighterMap.get(key);
        if (highlighter == null) {
            highlighter = new FastVectorHighlighter(true, false, fragListBuilder, fragmentsBuilder);
            highlighterMap.put(key,highlighter);
        }
        return Tuple.tuple(key, highlighter);
    }
}
