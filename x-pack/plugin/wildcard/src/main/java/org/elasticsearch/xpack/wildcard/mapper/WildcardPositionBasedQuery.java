/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queries.XIntervals;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper.WildcardFieldType.PatternStructure;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Objects;

public class WildcardPositionBasedQuery extends Query{

    private final String field;
    private final PatternStructure wildcardPattern;
    private final int numChars;

    public WildcardPositionBasedQuery(String field, PatternStructure wildcardPattern, int numChars) {
        this.field = field;
        this.wildcardPattern = wildcardPattern;
        this.numChars = numChars;
    }
    
    
    private void addFragment(ArrayList<IntervalsSource> spans, int fragmentNum, IntervalsSource newFragment) {
        // TODO mixtures of ? and * in a sequence e.g. aa?*  are problematic because we only have the max gap 
        //(Integer.maxInt in this case) and not the min (which is 1 in this example).
        int precedingGapSize  = wildcardPattern.getPrecedingGapSize(fragmentNum);
        if ( precedingGapSize >0 && precedingGapSize < Integer.MAX_VALUE) {
            if (spans.size() == 0) {
                IntervalsSource wildcard = XIntervals.wildcard(new BytesRef(WildcardFieldMapper.TOKEN_START_OR_END_CHAR + "*"));
                IntervalsSource addedGap = Intervals.extend(wildcard, 0, precedingGapSize);
                IntervalsSource phrase = Intervals.phrase(addedGap, newFragment);
                spans.add(phrase);
                return;
            }
            IntervalsSource lastFragment = spans.get(spans.size()-1);
            
            IntervalsSource addedGap = Intervals.extend(lastFragment, 0, precedingGapSize);
            IntervalsSource phrase = Intervals.phrase(addedGap, newFragment);
            spans.set(spans.size()-1, phrase);
            
            
        } else {
            spans.add(newFragment);
        }
    }
    
    
    
    
    private String escapeWildcards(String s) {
        s = s.replace(Character.toString(WildcardQuery.WILDCARD_CHAR), 
                Character.toString(WildcardQuery.WILDCARD_ESCAPE)+Character.toString(WildcardQuery.WILDCARD_CHAR));
        s = s.replace(Character.toString(WildcardQuery.WILDCARD_STRING), 
                Character.toString(WildcardQuery.WILDCARD_ESCAPE)+Character.toString(WildcardQuery.WILDCARD_STRING));        
        return s;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        
        
        ArrayList<IntervalsSource> spans = new ArrayList<>();

        
        for (int i = 0; i < wildcardPattern.fragments.length; i++) {
            String fragment = wildcardPattern.fragments[i];
            int fLength = fragment.length();
            if (fLength == 0) {
                continue;
            }
            
            // Add any start/end of string character
            if (i == 0 && wildcardPattern.openStart == false) {
                // Start-of-string anchored (is not a leading wildcard)
                fragment = WildcardFieldMapper.TOKEN_START_OR_END_CHAR + fragment;
            }
            if (wildcardPattern.openEnd == false && i == wildcardPattern.fragments.length - 1) {
                // End-of-string anchored (is not a trailing wildcard)
                fragment = fragment + WildcardFieldMapper.TOKEN_START_OR_END_CHAR;
            }
            
            if (fragment.length() == numChars) {
                IntervalsSource addedGap = Intervals.extend(Intervals.term(fragment), 0, fragment.length()-1);                
                addFragment(spans, i, addedGap);
            } else if (fragment.length() > numChars) {
                // Break fragment into multiple Ngrams      
                
                
                ArrayList<IntervalsSource> fragmentRun = new ArrayList<>();
                KeywordTokenizer kt = new KeywordTokenizer(256);
                kt.setReader(new StringReader(fragment));
                TokenFilter filter = new NGramTokenFilter(kt, numChars, numChars, false);
                CharTermAttribute termAtt = filter.addAttribute(CharTermAttribute.class);
                try {
                    filter.reset();
                    int nextRequiredCoverage = 0;
                    int charPos = 0;
                    int endOfOptimisableSection = fragment.length() - (numChars * 2);
                    
                    while (filter.incrementToken()) {
                        if (charPos < endOfOptimisableSection) {
                            if (charPos == nextRequiredCoverage) {
                                IntervalsSource iTerm = Intervals.term(termAtt.toString());
                                fragmentRun.add(Intervals.extend(iTerm, 0, termAtt.length() - 1));
                                // optimise - skip unnecessary overlapping tokens
                                nextRequiredCoverage = charPos + termAtt.length();
                            }
                        } else {
                            // We are into the tail of the string that can't be optimised by skipping
                            if (charPos >= nextRequiredCoverage) {
                                fragmentRun.add(Intervals.term(termAtt.toString()));
                                if (charPos + termAtt.length() >= fragment.length()) {
                                    // we've achieved full coverage of the pattern now
                                    break;
                                }
                            }
                        }
                        charPos++;
                    }
                    kt.end();
                    kt.close();
                } catch(IOException ioe) {
                    throw new ElasticsearchParseException("Error parsing wildcard query pattern fragment ["+fragment+"]");
                }
                
                IntervalsSource phrase = Intervals.phrase(fragmentRun.toArray(new IntervalsSource[0]));
                IntervalsSource addedGap = Intervals.extend(phrase, 0, numChars - 1);
                addFragment(spans, i, addedGap);
                
            } else {
                // fragment is smaller than smallest ngram size
                if (wildcardPattern.openEnd || i < wildcardPattern.fragments.length - 1) {
                    // fragment occurs mid-string so will need a wildcard query
                    IntervalsSource wildcard = XIntervals.wildcard(new BytesRef(escapeWildcards(fragment) + "*"));
                    
                    IntervalsSource addedGap = Intervals.extend(wildcard, 0, fragment.length()-1);
                    addFragment(spans, i, addedGap);
                } else {
                    // fragment occurs at end of string so can rely on Jim's indexing rule to optimise 
                    // *foo by indexing smaller ngrams at the end of a string
                    IntervalsSource addedGap = Intervals.extend(Intervals.term(fragment), 0, fragment.length()-1);
                    addFragment(spans, i, addedGap);
                }
            }
        }
        
        if (wildcardPattern.lastGap > 0 && wildcardPattern.lastGap < Integer.MAX_VALUE) {
            IntervalsSource lastFragment = spans.get(spans.size() - 1);
            IntervalsSource addedGap = Intervals.extend(lastFragment, 0, wildcardPattern.lastGap);
            IntervalsSource fieldEnd = Intervals.term(new BytesRef("" + WildcardFieldMapper.TOKEN_START_OR_END_CHAR));
            IntervalsSource phrase = Intervals.phrase(addedGap, fieldEnd);
            spans.set(spans.size() - 1, phrase);
        }
                
        if (spans.size()==1) {
            IntervalQuery iq = new IntervalQuery(field, spans.get(0));
            return iq;            
        }
        
        IntervalQuery iq = new IntervalQuery(field, Intervals.ordered(spans.toArray(new IntervalsSource[0])));
        return iq;
    }

    @Override
    public boolean equals(Object obj) {
        WildcardPositionBasedQuery other = (WildcardPositionBasedQuery) obj;
        return Objects.equals(field, other.field)  && Objects.equals(wildcardPattern, other.wildcardPattern) &&
                Objects.equals(numChars, other.numChars);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, wildcardPattern, numChars);
    }




    @Override
    public String toString(String field) {
        return field + ":" + wildcardPattern.pattern;
    }

}
