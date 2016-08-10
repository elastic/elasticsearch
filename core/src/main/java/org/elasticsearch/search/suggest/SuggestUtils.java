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
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordFrequencyComparator;
import org.apache.lucene.search.spell.SuggestWordQueue;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.FastCharArrayReader;

import java.io.IOException;
import java.util.Comparator;

public final class SuggestUtils {
    private static final Comparator<SuggestWord> LUCENE_FREQUENCY = new SuggestWordFrequencyComparator();
    private static final Comparator<SuggestWord> SCORE_COMPARATOR = SuggestWordQueue.DEFAULT_COMPARATOR;

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

    public abstract static class TokenConsumer {
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
        try (TokenStream ts = analyzer.tokenStream(
                                  field, new FastCharArrayReader(toAnalyze.chars, toAnalyze.offset, toAnalyze.length))) {
             return analyze(ts, consumer);
        }
    }

    /** NOTE: this method closes the TokenStream, even on exception, which is awkward
     *  because really the caller who called {@link Analyzer#tokenStream} should close it,
     *  but when trying that there are recursion issues when we try to use the same
     *  TokenStream twice in the same recursion... */
    public static int analyze(TokenStream stream, TokenConsumer consumer) throws IOException {
        int numTokens = 0;
        boolean success = false;
        try {
            stream.reset();
            consumer.reset(stream);
            while (stream.incrementToken()) {
                consumer.nextToken();
                numTokens++;
            }
            consumer.end();
        } finally {
            if (success) {
                stream.close();
            } else {
                IOUtils.closeWhileHandlingException(stream);
            }
        }
        return numTokens;
    }

    public static class Fields {
        public static final ParseField STRING_DISTANCE = new ParseField("string_distance");
        public static final ParseField SUGGEST_MODE = new ParseField("suggest_mode");
        public static final ParseField MAX_EDITS = new ParseField("max_edits");
        public static final ParseField MAX_INSPECTIONS = new ParseField("max_inspections");
        // TODO some of these constants are the same as MLT constants and
        // could be moved to a shared class for consistency
        public static final ParseField MAX_TERM_FREQ = new ParseField("max_term_freq");
        public static final ParseField PREFIX_LENGTH = new ParseField("prefix_length", "prefix_len");
        public static final ParseField MIN_WORD_LENGTH = new ParseField("min_word_length", "min_word_len");
        public static final ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
        public static final ParseField SHARD_SIZE = new ParseField("shard_size");
        public static final ParseField ANALYZER = new ParseField("analyzer");
        public static final ParseField FIELD = new ParseField("field");
        public static final ParseField SIZE = new ParseField("size");
        public static final ParseField SORT = new ParseField("sort");
        public static final ParseField ACCURACY = new ParseField("accuracy");
   }
}
