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
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.search.suggest.analyzing.XFuzzySuggester;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.io.IOException;

/**
 * A form of {@link CompletionSuggestionBuilder} that supports fuzzy queries allowing 
 * matches on typos. 
 * Various settings control when and how fuzziness is counted.
 */
public class CompletionSuggestionFuzzyBuilder extends SuggestBuilder.SuggestionBuilder<CompletionSuggestionFuzzyBuilder> {

    public CompletionSuggestionFuzzyBuilder(String name) {
        super(name, "completion");
    }

    private Fuzziness fuzziness = Fuzziness.ONE;
    private boolean fuzzyTranspositions = XFuzzySuggester.DEFAULT_TRANSPOSITIONS;
    private int fuzzyMinLength = XFuzzySuggester.DEFAULT_MIN_FUZZY_LENGTH;
    private int fuzzyPrefixLength = XFuzzySuggester.DEFAULT_NON_FUZZY_PREFIX;
    private boolean unicodeAware = XFuzzySuggester.DEFAULT_UNICODE_AWARE;

    public Fuzziness getFuzziness() {
        return fuzziness;
    }

    /**
     * Sets the level of fuzziness used to create suggestions using a {@link Fuzziness} instance.
     * The default value is {@link Fuzziness#ONE} which allows for an "edit distance" of one.
     */
    public CompletionSuggestionFuzzyBuilder setFuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    public boolean isFuzzyTranspositions() {
        return fuzzyTranspositions;
    }

    /**
     * Sets if transpositions (swapping one character for another) counts as one character 
     * change or two.
     * Defaults to true, meaning it uses the fuzzier option of counting transpositions as 
     * a single change.   
     */
    public CompletionSuggestionFuzzyBuilder setFuzzyTranspositions(boolean fuzzyTranspositions) {
        this.fuzzyTranspositions = fuzzyTranspositions;
        return this;
    }

    public int getFuzzyMinLength() {
        return fuzzyMinLength;
    }

    /**
     * Sets the minimum length of input string before fuzzy suggestions are returned, defaulting
     * to 3.   
     */
    public CompletionSuggestionFuzzyBuilder setFuzzyMinLength(int fuzzyMinLength) {
        this.fuzzyMinLength = fuzzyMinLength;
        return this;
    }

    public int getFuzzyPrefixLength() {
        return fuzzyPrefixLength;
    }

    /**
     * Sets the minimum length of the input, which is not checked for fuzzy alternatives, defaults to 1
     */
    public CompletionSuggestionFuzzyBuilder setFuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
        return this;
    }

    public boolean isUnicodeAware() {
        return unicodeAware;
    }

    /**
     * Set to true if all measurements (like edit distance, transpositions and lengths) are in unicode 
     * code points (actual letters) instead of bytes. Default is false.
     */
    public CompletionSuggestionFuzzyBuilder setUnicodeAware(boolean unicodeAware) {
        this.unicodeAware = unicodeAware;
        return this;
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("fuzzy");

        if (fuzziness != Fuzziness.ONE) {
            fuzziness.toXContent(builder, params);
        }
        if (fuzzyTranspositions != XFuzzySuggester.DEFAULT_TRANSPOSITIONS) {
            builder.field("transpositions", fuzzyTranspositions);
        }
        if (fuzzyMinLength != XFuzzySuggester.DEFAULT_MIN_FUZZY_LENGTH) {
            builder.field("min_length", fuzzyMinLength);
        }
        if (fuzzyPrefixLength != XFuzzySuggester.DEFAULT_NON_FUZZY_PREFIX) {
            builder.field("prefix_length", fuzzyPrefixLength);
        }
        if (unicodeAware != XFuzzySuggester.DEFAULT_UNICODE_AWARE) {
            builder.field("unicode_aware", unicodeAware);
        }

        builder.endObject();
        return builder;
    }
}
