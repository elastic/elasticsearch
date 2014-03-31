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
package org.elasticsearch.search.suggest.freetext;

import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.Suggester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class FreeTextSuggester extends Suggester<FreeTextSuggestionContext> {

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
                FreeTextSuggestionContext suggestionContext, IndexReader indexReader, CharsRef spare) throws IOException {

        if (suggestionContext.mapper().postingsFormatProvider() == null || !"freetext".equals(suggestionContext.mapper().postingsFormatProvider().get().getName())) {
            throw new ElasticsearchException("Field [" + suggestionContext.getField() + "] is not configured with freetext postings format");
        }

        Suggest.Suggestion suggestion = new Suggest.Suggestion(name, suggestionContext.getSize());
        UnicodeUtil.UTF8toUTF16(suggestionContext.getText(), spare);

        Suggest.Suggestion.Entry suggestEntry = new Suggest.Suggestion.Entry(new StringText(spare.toString()), 0, spare.length());
        suggestion.addTerm(suggestEntry);

        String fieldName = suggestionContext.getField();
        Map<String, Suggest.Suggestion.Entry.Option> results = Maps.newHashMapWithExpectedSize(indexReader.leaves().size() * suggestionContext.getSize());

        ShingleTokenFilterFactory.Factory shingleFilterFactory = SuggestUtils.getShingleFilterFactory(suggestionContext.mapper().indexAnalyzer());
        String separator = null;
        if (shingleFilterFactory != null && Strings.hasLength(shingleFilterFactory.getTokenSeparator())) {
            separator = shingleFilterFactory.getTokenSeparator();
        }

        for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
            AtomicReader atomicReader = atomicReaderContext.reader();
            Terms terms = atomicReader.fields().terms(fieldName);
            if (terms instanceof FreeTextPostingsFormat.FreeTextTerms) {
                final FreeTextPostingsFormat.FreeTextTerms lookupTerms = (FreeTextPostingsFormat.FreeTextTerms) terms;
                final Lookup lookup = lookupTerms.getLookup(suggestionContext);
                if (lookup == null) {
                    // we don't have a lookup for this segment.. this might be possible if a merge dropped all
                    // docs from the segment that had a value in this segment.
                    continue;
                }
                List<Lookup.LookupResult> lookupResults = lookup.lookup(spare, false, suggestionContext.getSize());
                for (Lookup.LookupResult res : lookupResults) {
                    String key = res.key.toString();
                    final float score = res.value;
                    if (separator != null && suggestionContext.getSeparator() != null && !separator.equals(suggestionContext.getSeparator())) {
                        key = key.replaceAll("\\" + separator, suggestionContext.getSeparator());
                    }
                    // TODO DUPS ?
                    final Suggest.Suggestion.Entry.Option option = new Suggest.Suggestion.Entry.Option(new StringText(key), null, score);
                    results.put(key, option);
                }
            }
        }

        // TODO SORTING?
        final List<Suggest.Suggestion.Entry.Option> options = new ArrayList<Suggest.Suggestion.Entry.Option>(results.values());
        int optionCount = Math.min(suggestionContext.getSize(), options.size());
        for (int i = 0 ; i < optionCount ; i++) {
            suggestEntry.addOption(options.get(i));
        }

        return suggestion;
    }

    @Override
    public String[] names() {
        return new String[] { "freetext" };
    }

    @Override
    public SuggestContextParser getContextParser() {
        return new FreeTextSuggestParser(this);
    }
}
