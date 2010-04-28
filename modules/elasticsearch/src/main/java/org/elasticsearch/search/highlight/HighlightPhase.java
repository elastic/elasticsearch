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

import org.apache.lucene.search.vectorhighlight.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.util.gcommon.collect.ImmutableMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

        FragListBuilder fragListBuilder = new SimpleFragListBuilder();
        FragmentsBuilder fragmentsBuilder;
        if (context.highlight().scoreOrdered()) {
            fragmentsBuilder = new ScoreOrderFragmentsBuilder(context.highlight().preTags(), context.highlight().postTags());
        } else {
            fragmentsBuilder = new SimpleFragmentsBuilder(context.highlight().preTags(), context.highlight().postTags());
        }
        FastVectorHighlighter highlighter = new FastVectorHighlighter(true, false, fragListBuilder, fragmentsBuilder);

        CustomFieldQuery.reader.set(context.searcher().getIndexReader());
        CustomFieldQuery.highlightFilters.set(context.highlight().highlightFilter());

        FieldQuery fieldQuery = new CustomFieldQuery(context.query(), highlighter);
        for (SearchHit hit : context.fetchResult().hits().hits()) {
            InternalSearchHit internalHit = (InternalSearchHit) hit;

            DocumentMapper documentMapper = context.mapperService().type(internalHit.type());
            int docId = internalHit.docId();

            Map<String, HighlightField> highlightFields = new HashMap<String, HighlightField>();
            for (SearchContextHighlight.ParsedHighlightField parsedHighlightField : context.highlight().fields()) {
                String indexName = parsedHighlightField.field();
                FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(parsedHighlightField.field());
                if (mapper != null) {
                    indexName = mapper.names().indexName();
                }
                String[] fragments = null;
                try {
                    fragments = highlighter.getBestFragments(fieldQuery, context.searcher().getIndexReader(), docId, indexName, parsedHighlightField.fragmentCharSize(), parsedHighlightField.numberOfFragments());
                } catch (IOException e) {
                    throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + parsedHighlightField.field() + "]", e);
                }
                HighlightField highlightField = new HighlightField(parsedHighlightField.field(), fragments);
                highlightFields.put(highlightField.name(), highlightField);
            }

            internalHit.highlightFields(highlightFields);
        }
    }
}
