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

import com.google.common.collect.Maps;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.*;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.highlight.vectorhighlight.SimpleFragmentsBuilder;
import org.elasticsearch.search.highlight.vectorhighlight.SourceScoreOrderFragmentsBuilder;
import org.elasticsearch.search.highlight.vectorhighlight.SourceSimpleFragmentsBuilder;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class FastVectorHighlighter implements Highlighter {

    private static final SimpleBoundaryScanner DEFAULT_BOUNDARY_SCANNER = new SimpleBoundaryScanner();

    private static final String CACHE_KEY = "highlight-fsv";
    private final Boolean termVectorMultiValue;

    @Inject
    public FastVectorHighlighter(Settings settings) {
        this.termVectorMultiValue = settings.getAsBoolean("search.highlight.term_vector_multi_value", true);
    }

    @Override
    public String[] names() {
        return new String[]{"fvh", "fast-vector-highlighter"};
    }

    @Override
    public HighlightField highlight(HighlighterContext highlighterContext) {
        SearchContextHighlight.Field field = highlighterContext.field;
        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;
        FieldMapper<?> mapper = highlighterContext.mapper;

        Encoder encoder = field.encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;

        if (!hitContext.cache().containsKey(CACHE_KEY)) {
            hitContext.cache().put(CACHE_KEY, new HighlighterEntry());
        }
        HighlighterEntry cache = (HighlighterEntry) hitContext.cache().get(CACHE_KEY);

        try {
            CustomFieldQuery fieldQuery;
            if (field.requireFieldMatch()) {
                if (cache.fieldMatchFieldQuery == null) {
                    // we use top level reader to rewrite the query against all readers, with use caching it across hits (and across readers...)
                    cache.fieldMatchFieldQuery = new CustomFieldQuery(highlighterContext.query.originalQuery(), hitContext.topLevelReader(), true, field.requireFieldMatch());
                }
                fieldQuery = cache.fieldMatchFieldQuery;
            } else {
                if (cache.noFieldMatchFieldQuery == null) {
                    // we use top level reader to rewrite the query against all readers, with use caching it across hits (and across readers...)
                    cache.noFieldMatchFieldQuery = new CustomFieldQuery(highlighterContext.query.originalQuery(), hitContext.topLevelReader(), true, field.requireFieldMatch());
                }
                fieldQuery = cache.noFieldMatchFieldQuery;
            }

            MapperHighlightEntry entry = cache.mappers.get(mapper);
            if (entry == null) {
                FragListBuilder fragListBuilder;
                BaseFragmentsBuilder fragmentsBuilder;

                BoundaryScanner boundaryScanner = DEFAULT_BOUNDARY_SCANNER;
                if (field.boundaryMaxScan() != SimpleBoundaryScanner.DEFAULT_MAX_SCAN || field.boundaryChars() != SimpleBoundaryScanner.DEFAULT_BOUNDARY_CHARS) {
                    boundaryScanner = new SimpleBoundaryScanner(field.boundaryMaxScan(), field.boundaryChars());
                }

                if (field.numberOfFragments() == 0) {
                    fragListBuilder = new SingleFragListBuilder();

                    if (!field.forceSource() && mapper.fieldType().stored()) {
                        fragmentsBuilder = new SimpleFragmentsBuilder(mapper, field.preTags(), field.postTags(), boundaryScanner);
                    } else {
                        fragmentsBuilder = new SourceSimpleFragmentsBuilder(mapper, field.preTags(), field.postTags(), boundaryScanner);
                    }
                } else {
                    fragListBuilder = field.fragmentOffset() == -1 ? new SimpleFragListBuilder() : new SimpleFragListBuilder(field.fragmentOffset());
                    if (field.scoreOrdered()) {
                        if (!field.forceSource() && mapper.fieldType().stored()) {
                            fragmentsBuilder = new ScoreOrderFragmentsBuilder(field.preTags(), field.postTags(), boundaryScanner);
                        } else {
                            fragmentsBuilder = new SourceScoreOrderFragmentsBuilder(mapper, field.preTags(), field.postTags(), boundaryScanner);
                        }
                    } else {
                        if (!field.forceSource() && mapper.fieldType().stored()) {
                            fragmentsBuilder = new SimpleFragmentsBuilder(mapper, field.preTags(), field.postTags(), boundaryScanner);
                        } else {
                            fragmentsBuilder = new SourceSimpleFragmentsBuilder(mapper, field.preTags(), field.postTags(), boundaryScanner);
                        }
                    }
                }
                fragmentsBuilder.setDiscreteMultiValueHighlighting(termVectorMultiValue);
                entry = new MapperHighlightEntry();
                entry.fragListBuilder = fragListBuilder;
                entry.fragmentsBuilder = fragmentsBuilder;
                if (cache.fvh == null) {
                    // parameters to FVH are not requires since:
                    // first two booleans are not relevant since they are set on the CustomFieldQuery (phrase and fieldMatch)
                    // fragment builders are used explicitly
                    cache.fvh = new org.apache.lucene.search.vectorhighlight.FastVectorHighlighter();
                }
                CustomFieldQuery.highlightFilters.set(field.highlightFilter());
                cache.mappers.put(mapper, entry);
            }
            cache.fvh.setPhraseLimit(field.phraseLimit());

            String[] fragments;

            // a HACK to make highlighter do highlighting, even though its using the single frag list builder
            int numberOfFragments = field.numberOfFragments() == 0 ? Integer.MAX_VALUE : field.numberOfFragments();
            int fragmentCharSize = field.numberOfFragments() == 0 ? Integer.MAX_VALUE : field.fragmentCharSize();
            // The reader we highlight against.
            IndexReader reader = new DelegatingOrAnalyzingReaderForFVH(context, hitContext, field.forceSource(), fieldQuery);
            // Only send matched fields if they were requested to save time.
            if (field.matchedFields() != null && !field.matchedFields().isEmpty()) {
                fragments = cache.fvh.getBestFragments(fieldQuery, reader, hitContext.docId(), mapper.names().indexName(), field.matchedFields(), fragmentCharSize,
                        numberOfFragments, entry.fragListBuilder, entry.fragmentsBuilder, field.preTags(), field.postTags(), encoder);
            } else {
                fragments = cache.fvh.getBestFragments(fieldQuery, reader, hitContext.docId(), mapper.names().indexName(), fragmentCharSize,
                        numberOfFragments, entry.fragListBuilder, entry.fragmentsBuilder, field.preTags(), field.postTags(), encoder);
            }

            if (fragments != null && fragments.length > 0) {
                return new HighlightField(field.field(), StringText.convertFromStringArray(fragments));
            }

            int noMatchSize = highlighterContext.field.noMatchSize();
            if (noMatchSize > 0) {
                // Essentially we just request that a fragment is built from 0 to noMatchSize using the normal fragmentsBuilder
                FieldFragList fieldFragList = new SimpleFieldFragList(-1 /*ignored*/);
                fieldFragList.add(0, noMatchSize, Collections.<WeightedPhraseInfo>emptyList());
                fragments = entry.fragmentsBuilder.createFragments(reader, hitContext.docId(), mapper.names().indexName(),
                        fieldFragList, 1, field.preTags(), field.postTags(), encoder);
                if (fragments != null && fragments.length > 0) {
                    return new HighlightField(field.field(), StringText.convertFromStringArray(fragments));
                }
            }

            return null;

        } catch (Exception e) {
            throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
        }
    }

    private static class MapperHighlightEntry {
        public FragListBuilder fragListBuilder;
        public FragmentsBuilder fragmentsBuilder;

        public org.apache.lucene.search.highlight.Highlighter highlighter;
    }

    private static class HighlighterEntry {
        public org.apache.lucene.search.vectorhighlight.FastVectorHighlighter fvh;
        public CustomFieldQuery noFieldMatchFieldQuery;
        public CustomFieldQuery fieldMatchFieldQuery;
        public Map<FieldMapper, MapperHighlightEntry> mappers = Maps.newHashMap();
    }
    
    private static class DelegatingOrAnalyzingReaderForFVH extends AbstractDelegatingOrAnalyzingReader {
        public DelegatingOrAnalyzingReaderForFVH(SearchContext searchContext, HitContext hitContext, boolean forceSource,
                TermSetSource termSetSource) {
            super(searchContext, hitContext, forceSource, termSetSource);
        }

        @Override
        public Fields getTermVectors(int docId) throws IOException {
            // If we can push this call until we have the field so we an make an educated guess
            // (using the mapper) as to whether there might be term vectors for the field.
            Fields real = super.getTermVectors(docId);
            if (real == null) {
                return new AnalyzingFields();
            }
            return new DelegatingOrAnalyzingFields(real);
        }
    }
}
