/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
package org.elasticsearch.search.suggest;

import java.io.IOException;
import java.util.Comparator;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.search.spell.LuceneLevenshteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordFrequencyComparator;
import org.apache.lucene.search.spell.SuggestWordQueue;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.io.FastCharArrayReader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

public final class SuggestUtils {
    public static Comparator<SuggestWord> LUCENE_FREQUENCY = new SuggestWordFrequencyComparator();
    public static Comparator<SuggestWord> SCORE_COMPARATOR = SuggestWordQueue.DEFAULT_COMPARATOR;
    
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
                throw new ElasticSearchIllegalArgumentException("Illegal suggest sort: " + suggestion.sort());
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
    
    public static BytesRef join(BytesRef separator, BytesRef result, BytesRef... toJoin) {
        int len = separator.length * toJoin.length - 1;
        for (BytesRef br : toJoin) {
            len += br.length;
        }
        
        result.grow(len);
        return joinPreAllocated(separator, result, toJoin);
    }
    
    public static BytesRef joinPreAllocated(BytesRef separator, BytesRef result, BytesRef... toJoin) {
        result.length = 0;
        result.offset = 0;
        for (int i = 0; i < toJoin.length - 1; i++) {
            BytesRef br = toJoin[i];
            System.arraycopy(br.bytes, br.offset, result.bytes, result.offset, br.length);
            result.offset += br.length;
            System.arraycopy(separator.bytes, separator.offset, result.bytes, result.offset, separator.length);
            result.offset += separator.length;
        }
        final BytesRef br = toJoin[toJoin.length-1];
        System.arraycopy(br.bytes, br.offset, result.bytes, result.offset, br.length);
        
        result.length = result.offset + br.length;
        result.offset = 0;
        return result;
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
        
        protected BytesRef fillBytesRef(BytesRef spare) {
            spare.offset = 0;
            spare.length = spare.bytes.length;
            char[] source = charTermAttr.buffer();
            UnicodeUtil.UTF16toUTF8(source, 0, charTermAttr.length(), spare);
            return spare;
        }
        
        public abstract void nextToken() throws IOException;

        public void end() {}
    }
    
    public static int analyze(Analyzer analyzer, BytesRef toAnalyze, String field, TokenConsumer consumer, CharsRef spare) throws IOException {
        UnicodeUtil.UTF8toUTF16(toAnalyze, spare);
        return analyze(analyzer, spare, field, consumer);
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
            throw new ElasticSearchIllegalArgumentException("Illegal suggest mode " + suggestMode);
        }
    }

    public static Suggest.Suggestion.Sort resolveSort(String sortVal) {
        if ("score".equals(sortVal)) {
            return Suggest.Suggestion.Sort.SCORE;
        } else if ("frequency".equals(sortVal)) {
            return Suggest.Suggestion.Sort.FREQUENCY;
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal suggest sort " + sortVal);
        }
    }

    public static StringDistance resolveDistance(String distanceVal) {
        if ("internal".equals(distanceVal)) {
            return DirectSpellChecker.INTERNAL_LEVENSHTEIN;
        } else if ("damerau_levenshtein".equals(distanceVal) || "damerauLevenshtein".equals(distanceVal)) {
            return new LuceneLevenshteinDistance();
        } else if ("levenstein".equals(distanceVal)) {
            return new LevensteinDistance();
        } else if ("jarowinkler".equals(distanceVal)) {
            return new JaroWinklerDistance();
        } else if ("ngram".equals(distanceVal)) {
            return new NGramDistance();
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal distance option " + distanceVal);
        }
    }
    
    public static boolean parseDirectSpellcheckerSettings(XContentParser parser, String fieldName,
                DirectSpellcheckerSettings suggestion) throws IOException {
            if ("accuracy".equals(fieldName)) {
                suggestion.accuracy(parser.floatValue());
            } else if ("suggest_mode".equals(fieldName) || "suggestMode".equals(fieldName)) {
                suggestion.suggestMode(SuggestUtils.resolveSuggestMode(parser.text()));
            } else if ("sort".equals(fieldName)) {
                suggestion.sort(SuggestUtils.resolveSort(parser.text()));
            } else if ("string_distance".equals(fieldName) || "stringDistance".equals(fieldName)) {
                suggestion.stringDistance(SuggestUtils.resolveDistance(parser.text()));
            } else if ("max_edits".equals(fieldName) || "maxEdits".equals(fieldName)) {
                suggestion.maxEdits(parser.intValue());
                if (suggestion.maxEdits() < 1 || suggestion.maxEdits() > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
                    throw new ElasticSearchIllegalArgumentException("Illegal max_edits value " + suggestion.maxEdits());
                }
            } else if ("max_inspections".equals(fieldName) || "maxInspections".equals(fieldName)) {
                suggestion.maxInspections(parser.intValue());
            } else if ("max_term_freq".equals(fieldName) || "maxTermFreq".equals(fieldName)) {
                suggestion.maxTermFreq(parser.floatValue());
            } else if ("prefix_len".equals(fieldName) || "prefixLen".equals(fieldName)) {
                suggestion.prefixLength(parser.intValue());
            } else if ("min_word_len".equals(fieldName) || "minWordLen".equals(fieldName)) {
                suggestion.minQueryLength(parser.intValue());
            } else if ("min_doc_freq".equals(fieldName) || "minDocFreq".equals(fieldName)) {
                suggestion.minDocFreq(parser.floatValue());
            } else {
                return false;
            }
            return true;
    }
    
    public static boolean parseSuggestContext(XContentParser parser, MapperService mapperService, String fieldName,
            SuggestionSearchContext.SuggestionContext suggestion) throws IOException {
        
        if ("analyzer".equals(fieldName)) {
            String analyzerName = parser.text();
            Analyzer analyzer = mapperService.analysisService().analyzer(analyzerName);
            if (analyzer == null) {
                throw new ElasticSearchIllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
            }
            suggestion.setAnalyzer(analyzer);
        } else if ("field".equals(fieldName)) {
            suggestion.setField(parser.text());
        } else if ("size".equals(fieldName)) {
            suggestion.setSize(parser.intValue());
        } else if ("shard_size".equals(fieldName) || "shardSize".equals(fieldName)) {
            suggestion.setShardSize(parser.intValue());
        } else {
           return false;
        }
        return true;
        
    }
    
    
    public static void verifySuggestion(MapperService mapperService, BytesRef globalText, SuggestionContext suggestion) {
        // Verify options and set defaults
        if (suggestion.getField() == null) {
            throw new ElasticSearchIllegalArgumentException("The required field option is missing");
        }
        if (suggestion.getText() == null) {
            if (globalText == null) {
                throw new ElasticSearchIllegalArgumentException("The required text option is missing");
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
