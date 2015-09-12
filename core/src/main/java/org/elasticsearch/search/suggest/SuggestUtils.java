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
package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.spell.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.FastCharArrayReader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

import java.io.IOException;
import java.util.Comparator;
import java.util.Locale;

public final class SuggestUtils {
    public static final Comparator<SuggestWord> LUCENE_FREQUENCY = new SuggestWordFrequencyComparator();
    public static final Comparator<SuggestWord> SCORE_COMPARATOR = SuggestWordQueue.DEFAULT_COMPARATOR;
    
    private SuggestUtils() {
        // utils!!
    }
    
    public static DirectSpellChecker getDirectSpellChecker(DirectSpellcheckerSettings suggestion) {
        
        DirectSpellChecker directSpellChecker = new DirectSpellChecker();
        directSpellChecker.setAccuracy(suggestion.accuracy());
        Comparator<SuggestWord> comparator;
        switch (suggestion.sort()) {
            case SCORE:
                comparator = SCORE_COMPARATOR;
                break;
            case FREQUENCY:
                comparator = LUCENE_FREQUENCY;
                break;
            default:
                throw new IllegalArgumentException("Illegal suggest sort: " + suggestion.sort());
        }
        directSpellChecker.setComparator(comparator);
        directSpellChecker.setDistance(suggestion.stringDistance());
        directSpellChecker.setMaxEdits(suggestion.maxEdits());
        directSpellChecker.setMaxInspections(suggestion.maxInspections());
        directSpellChecker.setMaxQueryFrequency(suggestion.maxTermFreq());
        directSpellChecker.setMinPrefix(suggestion.prefixLength());
        directSpellChecker.setMinQueryLength(suggestion.minWordLength());
        directSpellChecker.setThresholdFrequency(suggestion.minDocFreq());
        directSpellChecker.setLowerCaseTerms(false);
        return directSpellChecker;
    }
    
    public static BytesRef join(BytesRef separator, BytesRefBuilder result, BytesRef... toJoin) {
        result.clear();
        for (int i = 0; i < toJoin.length - 1; i++) {
            result.append(toJoin[i]);
            result.append(separator);
        }
        result.append(toJoin[toJoin.length-1]);
        return result.get();
    }
    
    public static abstract class TokenConsumer {
        protected CharTermAttribute charTermAttr;
        protected PositionIncrementAttribute posIncAttr;
        protected OffsetAttribute offsetAttr;
        
        public void reset(TokenStream stream) {
            charTermAttr = stream.addAttribute(CharTermAttribute.class);
            posIncAttr = stream.addAttribute(PositionIncrementAttribute.class);
            offsetAttr = stream.addAttribute(OffsetAttribute.class);
        }
        
        protected BytesRef fillBytesRef(BytesRefBuilder spare) {
            spare.copyChars(charTermAttr);
            return spare.get();
        }
        
        public abstract void nextToken() throws IOException;

        public void end() {}
    }
    
    public static int analyze(Analyzer analyzer, BytesRef toAnalyze, String field, TokenConsumer consumer, CharsRefBuilder spare) throws IOException {
        spare.copyUTF8Bytes(toAnalyze);
        return analyze(analyzer, spare.get(), field, consumer);
    }
    
    public static int analyze(Analyzer analyzer, CharsRef toAnalyze, String field, TokenConsumer consumer) throws IOException {
        TokenStream ts = analyzer.tokenStream(
                field, new FastCharArrayReader(toAnalyze.chars, toAnalyze.offset, toAnalyze.length)
        );
        return analyze(ts, consumer);
    }
    
    public static int analyze(TokenStream stream, TokenConsumer consumer) throws IOException {
        stream.reset();
        consumer.reset(stream);
        int numTokens = 0;
        while (stream.incrementToken()) {
            consumer.nextToken();
            numTokens++;
        }
        consumer.end();
        stream.close();
        return numTokens;
    }
    
    public static SuggestMode resolveSuggestMode(String suggestMode) {
        suggestMode = suggestMode.toLowerCase(Locale.US);
        if ("missing".equals(suggestMode)) {
            return SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
        } else if ("popular".equals(suggestMode)) {
            return SuggestMode.SUGGEST_MORE_POPULAR;
        } else if ("always".equals(suggestMode)) {
            return SuggestMode.SUGGEST_ALWAYS;
        } else {
            throw new IllegalArgumentException("Illegal suggest mode " + suggestMode);
        }
    }

    public static Suggest.Suggestion.Sort resolveSort(String sortVal) {
        if ("score".equals(sortVal)) {
            return Suggest.Suggestion.Sort.SCORE;
        } else if ("frequency".equals(sortVal)) {
            return Suggest.Suggestion.Sort.FREQUENCY;
        } else {
            throw new IllegalArgumentException("Illegal suggest sort " + sortVal);
        }
    }

    public static StringDistance resolveDistance(String distanceVal) {
        if ("internal".equals(distanceVal)) {
            return DirectSpellChecker.INTERNAL_LEVENSHTEIN;
        } else if ("damerau_levenshtein".equals(distanceVal) || "damerauLevenshtein".equals(distanceVal)) {
            return new LuceneLevenshteinDistance();
        } else if ("levenstein".equals(distanceVal)) {
            return new LevensteinDistance();
          //TODO Jaro and Winkler are 2 people - so apply same naming logic as damerau_levenshtein  
        } else if ("jarowinkler".equals(distanceVal)) {
            return new JaroWinklerDistance();
        } else if ("ngram".equals(distanceVal)) {
            return new NGramDistance();
        } else {
            throw new IllegalArgumentException("Illegal distance option " + distanceVal);
        }
    }
    
    public static class Fields {
        public static final ParseField STRING_DISTANCE = new ParseField("string_distance");
        public static final ParseField SUGGEST_MODE = new ParseField("suggest_mode");
        public static final ParseField MAX_EDITS = new ParseField("max_edits");
        public static final ParseField MAX_INSPECTIONS = new ParseField("max_inspections");
        // TODO some of these constants are the same as MLT constants and
        // could be moved to a shared class for maintaining consistency across
        // the platform
        public static final ParseField MAX_TERM_FREQ = new ParseField("max_term_freq");
        public static final ParseField PREFIX_LENGTH = new ParseField("prefix_length", "prefix_len");
        public static final ParseField MIN_WORD_LENGTH = new ParseField("min_word_length", "min_word_len");
        public static final ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
        public static final ParseField SHARD_SIZE = new ParseField("shard_size");
   }      
    
    public static boolean parseDirectSpellcheckerSettings(XContentParser parser, String fieldName,
                DirectSpellcheckerSettings suggestion, ParseFieldMatcher parseFieldMatcher) throws IOException {
            if ("accuracy".equals(fieldName)) {
                suggestion.accuracy(parser.floatValue());
            } else if (parseFieldMatcher.match(fieldName, Fields.SUGGEST_MODE)) {
                suggestion.suggestMode(SuggestUtils.resolveSuggestMode(parser.text()));
            } else if ("sort".equals(fieldName)) {
                suggestion.sort(SuggestUtils.resolveSort(parser.text()));
            } else if (parseFieldMatcher.match(fieldName, Fields.STRING_DISTANCE)) {
            suggestion.stringDistance(SuggestUtils.resolveDistance(parser.text()));
            } else if (parseFieldMatcher.match(fieldName, Fields.MAX_EDITS)) {
            suggestion.maxEdits(parser.intValue());
                if (suggestion.maxEdits() < 1 || suggestion.maxEdits() > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
                    throw new IllegalArgumentException("Illegal max_edits value " + suggestion.maxEdits());
                }
            } else if (parseFieldMatcher.match(fieldName, Fields.MAX_INSPECTIONS)) {
            suggestion.maxInspections(parser.intValue());
            } else if (parseFieldMatcher.match(fieldName, Fields.MAX_TERM_FREQ)) {
            suggestion.maxTermFreq(parser.floatValue());
            } else if (parseFieldMatcher.match(fieldName, Fields.PREFIX_LENGTH)) {
            suggestion.prefixLength(parser.intValue());
            } else if (parseFieldMatcher.match(fieldName, Fields.MIN_WORD_LENGTH)) {
            suggestion.minQueryLength(parser.intValue());
            } else if (parseFieldMatcher.match(fieldName, Fields.MIN_DOC_FREQ)) {
            suggestion.minDocFreq(parser.floatValue());
            } else {
                return false;
            }
            return true;
    }
    
    public static boolean parseSuggestContext(XContentParser parser, MapperService mapperService, String fieldName,
            SuggestionSearchContext.SuggestionContext suggestion, ParseFieldMatcher parseFieldMatcher) throws IOException {
        
        if ("analyzer".equals(fieldName)) {
            String analyzerName = parser.text();
            Analyzer analyzer = mapperService.analysisService().analyzer(analyzerName);
            if (analyzer == null) {
                throw new IllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
            }
            suggestion.setAnalyzer(analyzer);
        } else if ("field".equals(fieldName)) {
            suggestion.setField(parser.text());
        } else if ("size".equals(fieldName)) {
            suggestion.setSize(parser.intValue());
        } else if (parseFieldMatcher.match(fieldName, Fields.SHARD_SIZE)) {
            suggestion.setShardSize(parser.intValue());
        } else {
           return false;
        }
        return true;
        
    }
    
    
    public static void verifySuggestion(MapperService mapperService, BytesRef globalText, SuggestionContext suggestion) {
        // Verify options and set defaults
        if (suggestion.getField() == null) {
            throw new IllegalArgumentException("The required field option is missing");
        }
        if (suggestion.getText() == null) {
            if (globalText == null) {
                throw new IllegalArgumentException("The required text option is missing");
            }
            suggestion.setText(globalText);
        }
        if (suggestion.getAnalyzer() == null) {
            suggestion.setAnalyzer(mapperService.searchAnalyzer());
        }
        if (suggestion.getShardSize() == -1) {
            suggestion.setShardSize(Math.max(suggestion.getSize(), 5));
        }
    }
    
    
    public static ShingleTokenFilterFactory.Factory getShingleFilterFactory(Analyzer analyzer) {
        if (analyzer instanceof NamedAnalyzer) {
            analyzer = ((NamedAnalyzer)analyzer).analyzer();
        }
        if (analyzer instanceof CustomAnalyzer) {
            final CustomAnalyzer a = (CustomAnalyzer) analyzer;
            final TokenFilterFactory[] tokenFilters = a.tokenFilters();
            for (TokenFilterFactory tokenFilterFactory : tokenFilters) {
                if (tokenFilterFactory instanceof ShingleTokenFilterFactory) {
                    return ((ShingleTokenFilterFactory)tokenFilterFactory).getInnerFactory();
                } else if (tokenFilterFactory instanceof ShingleTokenFilterFactory.Factory) {
                    return (ShingleTokenFilterFactory.Factory) tokenFilterFactory;
                }
            }
        }
        return null;
    }
}
