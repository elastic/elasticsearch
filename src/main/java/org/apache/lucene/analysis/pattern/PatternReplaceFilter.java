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

package org.apache.lucene.analysis.pattern;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A TokenFilter which applies a Pattern to each token in the stream,
 * replacing match occurances with the specified replacement string.
 * <p/>
 * <p>
 * <b>Note:</b> Depending on the input and the pattern used and the input
 * TokenStream, this TokenFilter may produce Tokens whose text is the empty
 * string.
 * </p>
 *
 * @see Pattern
 */
public final class PatternReplaceFilter extends TokenFilter {
    private final Pattern p;
    private final String replacement;
    private final boolean all;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final Matcher m;

    /**
     * Constructs an instance to replace either the first, or all occurances
     *
     * @param in          the TokenStream to process
     * @param p           the patterm to apply to each Token
     * @param replacement the "replacement string" to substitute, if null a
     *                    blank string will be used. Note that this is not the literal
     *                    string that will be used, '$' and '\' have special meaning.
     * @param all         if true, all matches will be replaced otherwise just the first match.
     * @see Matcher#quoteReplacement
     */
    public PatternReplaceFilter(TokenStream in,
                                Pattern p,
                                String replacement,
                                boolean all) {
        super(in);
        this.p = p;
        this.replacement = (null == replacement) ? "" : replacement;
        this.all = all;
        this.m = p.matcher(termAtt);
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (!input.incrementToken()) return false;

        m.reset();
        if (m.find()) {
            // replaceAll/replaceFirst will reset() this previous find.
            String transformed = all ? m.replaceAll(replacement) : m.replaceFirst(replacement);
            termAtt.setEmpty().append(transformed);
        }

        return true;
    }

}
