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

package org.elasticsearch.search.suggest.term;

import org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase;

import java.io.IOException;

import static org.elasticsearch.search.suggest.term.TermSuggestionBuilder.SortBy;
import static org.elasticsearch.search.suggest.term.TermSuggestionBuilder.StringDistanceImpl;
import static org.elasticsearch.search.suggest.term.TermSuggestionBuilder.SuggestMode;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test the {@link TermSuggestionBuilder} class.
 */
public class TermSuggestionBuilderTests extends AbstractSuggestionBuilderTestCase<TermSuggestionBuilder> {

    @Override
    protected TermSuggestionBuilder randomSuggestionBuilder() {
        TermSuggestionBuilder testBuilder = new TermSuggestionBuilder(randomAsciiOfLength(10));
        maybeSet(testBuilder::suggestMode, randomSuggestMode());
        maybeSet(testBuilder::accuracy, randomFloat());
        maybeSet(testBuilder::sort, randomSort());
        maybeSet(testBuilder::stringDistance, randomStringDistance());
        maybeSet(testBuilder::maxEdits, randomIntBetween(1, 2));
        maybeSet(testBuilder::maxInspections, randomInt(Integer.MAX_VALUE));
        maybeSet(testBuilder::maxTermFreq, randomFloat());
        maybeSet(testBuilder::prefixLength, randomInt(Integer.MAX_VALUE));
        maybeSet(testBuilder::minWordLength, randomInt(Integer.MAX_VALUE));
        maybeSet(testBuilder::minDocFreq, randomFloat());
        return testBuilder;
    }

    private SuggestMode randomSuggestMode() {
        final int randomVal = randomIntBetween(0, 2);
        switch (randomVal) {
            case 0: return SuggestMode.MISSING;
            case 1: return SuggestMode.POPULAR;
            case 2: return SuggestMode.ALWAYS;
            default: throw new IllegalArgumentException("No suggest mode with an ordinal of " + randomVal);
        }
    }

    private SortBy randomSort() {
        int randomVal = randomIntBetween(0, 1);
        switch (randomVal) {
            case 0: return SortBy.SCORE;
            case 1: return SortBy.FREQUENCY;
            default: throw new IllegalArgumentException("No sort mode with an ordinal of " + randomVal);
        }
    }

    private StringDistanceImpl randomStringDistance() {
        int randomVal = randomIntBetween(0, 4);
        switch (randomVal) {
            case 0: return StringDistanceImpl.INTERNAL;
            case 1: return StringDistanceImpl.DAMERAU_LEVENSHTEIN;
            case 2: return StringDistanceImpl.LEVENSTEIN;
            case 3: return StringDistanceImpl.JAROWINKLER;
            case 4: return StringDistanceImpl.NGRAM;
            default: throw new IllegalArgumentException("No string distance algorithm with an ordinal of " + randomVal);
        }
    }

    @Override
    protected void mutateSpecificParameters(TermSuggestionBuilder builder) throws IOException {
        switch (randomIntBetween(0, 9)) {
            case 0:
                builder.suggestMode(randomValueOtherThan(builder.suggestMode(), () -> randomSuggestMode()));
                break;
            case 1:
                builder.accuracy(randomValueOtherThan(builder.accuracy(), () -> randomFloat()));
                break;
            case 2:
                builder.sort(randomValueOtherThan(builder.sort(), () -> randomSort()));
                break;
            case 3:
                builder.stringDistance(randomValueOtherThan(builder.stringDistance(), () -> randomStringDistance()));
                break;
            case 4:
                builder.maxEdits(randomValueOtherThan(builder.maxEdits(), () -> randomIntBetween(1, 2)));
                break;
            case 5:
                builder.maxInspections(randomValueOtherThan(builder.maxInspections(), () -> randomInt(Integer.MAX_VALUE)));
                break;
            case 6:
                builder.maxTermFreq(randomValueOtherThan(builder.maxTermFreq(), () -> randomFloat()));
                break;
            case 7:
                builder.prefixLength(randomValueOtherThan(builder.prefixLength(), () -> randomInt(Integer.MAX_VALUE)));
                break;
            case 8:
                builder.minWordLength(randomValueOtherThan(builder.minWordLength(), () -> randomInt(Integer.MAX_VALUE)));
                break;
            case 9:
                builder.minDocFreq(randomValueOtherThan(builder.minDocFreq(), () -> randomFloat()));
                break;
            default:
                break; // do nothing
        }
    }

    public void testInvalidParameters() throws IOException {
        TermSuggestionBuilder builder = new TermSuggestionBuilder(randomAsciiOfLength(10));
        // test invalid accuracy values
        try {
            builder.accuracy(-0.5f);
            fail("Should not allow accuracy to be set to a negative value.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.accuracy(1.1f);
            fail("Should not allow accuracy to be greater than 1.0.");
        } catch (IllegalArgumentException e) {
        }
        // test invalid max edit distance values
        try {
            builder.maxEdits(0);
            fail("Should not allow maxEdits to be less than 1.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.maxEdits(-1);
            fail("Should not allow maxEdits to be a negative value.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.maxEdits(3);
            fail("Should not allow maxEdits to be greater than 2.");
        } catch (IllegalArgumentException e) {
        }
        // test invalid max inspections values
        try {
            builder.maxInspections(-1);
            fail("Should not allow maxInspections to be a negative value.");
        } catch (IllegalArgumentException e) {
        }
        // test invalid max term freq values
        try {
            builder.maxTermFreq(-0.5f);
            fail("Should not allow max term freq to be a negative value.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.maxTermFreq(1.5f);
            fail("If max term freq is greater than 1, it must be a whole number.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.maxTermFreq(2.0f); // this should be allowed
        } catch (IllegalArgumentException e) {
            fail("A max term freq greater than 1 that is a whole number should be allowed.");
        }
        // test invalid min doc freq values
        try {
            builder.minDocFreq(-0.5f);
            fail("Should not allow min doc freq to be a negative value.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.minDocFreq(1.5f);
            fail("If min doc freq is greater than 1, it must be a whole number.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.minDocFreq(2.0f); // this should be allowed
        } catch (IllegalArgumentException e) {
            fail("A min doc freq greater than 1 that is a whole number should be allowed.");
        }
        // test invalid min word length values
        try {
            builder.minWordLength(0);
            fail("A min word length < 1 should not be allowed.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.minWordLength(-1);
            fail("Should not allow min word length to be a negative value.");
        } catch (IllegalArgumentException e) {
        }
        // test invalid prefix length values
        try {
            builder.prefixLength(-1);
            fail("Should not allow prefix length to be a negative value.");
        } catch (IllegalArgumentException e) {
        }
        // test invalid size values
        try {
            builder.size(0);
            fail("Size must be a positive value.");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.size(-1);
            fail("Size must be a positive value.");
        } catch (IllegalArgumentException e) {
        }
        // null values not allowed for enums
        try {
            builder.sort(null);
            fail("Should not allow setting a null sort value.");
        } catch (NullPointerException e) {
        }
        try {
            builder.stringDistance(null);
            fail("Should not allow setting a null string distance value.");
        } catch (NullPointerException e) {
        }
        try {
            builder.suggestMode(null);
            fail("Should not allow setting a null suggest mode value.");
        } catch (NullPointerException e) {
        }
    }

    public void testDefaultValuesSet() {
        TermSuggestionBuilder builder = new TermSuggestionBuilder(randomAsciiOfLength(10));
        assertThat(builder.accuracy(), notNullValue());
        assertThat(builder.maxEdits(), notNullValue());
        assertThat(builder.maxInspections(), notNullValue());
        assertThat(builder.maxTermFreq(), notNullValue());
        assertThat(builder.minDocFreq(), notNullValue());
        assertThat(builder.minWordLength(), notNullValue());
        assertThat(builder.prefixLength(), notNullValue());
        assertThat(builder.sort(), notNullValue());
        assertThat(builder.stringDistance(), notNullValue());
        assertThat(builder.suggestMode(), notNullValue());
    }

}
