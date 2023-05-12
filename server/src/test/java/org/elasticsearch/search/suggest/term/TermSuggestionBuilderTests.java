/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.term;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.core.Strings;
import org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase;
import org.elasticsearch.search.suggest.SortBy;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder.StringDistanceImpl;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder.SuggestMode;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_ACCURACY;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MAX_EDITS;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MAX_INSPECTIONS;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MAX_TERM_FREQ;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MIN_DOC_FREQ;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MIN_WORD_LENGTH;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_PREFIX_LENGTH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Test the {@link TermSuggestionBuilder} class.
 */
public class TermSuggestionBuilderTests extends AbstractSuggestionBuilderTestCase<TermSuggestionBuilder> {

    /**
     *  creates random suggestion builder, renders it to xContent and back to new instance that should be equal to original
     */
    @Override
    protected TermSuggestionBuilder randomSuggestionBuilder() {
        return randomTermSuggestionBuilder();
    }

    /**
     * Creates a random TermSuggestionBuilder
     */
    public static TermSuggestionBuilder randomTermSuggestionBuilder() {
        TermSuggestionBuilder testBuilder = new TermSuggestionBuilder(randomAlphaOfLengthBetween(2, 20));
        setCommonPropertiesOnRandomBuilder(testBuilder);
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

    private static SuggestMode randomSuggestMode() {
        final int randomVal = randomIntBetween(0, 2);
        return switch (randomVal) {
            case 0 -> SuggestMode.MISSING;
            case 1 -> SuggestMode.POPULAR;
            case 2 -> SuggestMode.ALWAYS;
            default -> throw new IllegalArgumentException("No suggest mode with an ordinal of " + randomVal);
        };
    }

    private static SortBy randomSort() {
        int randomVal = randomIntBetween(0, 1);
        return switch (randomVal) {
            case 0 -> SortBy.SCORE;
            case 1 -> SortBy.FREQUENCY;
            default -> throw new IllegalArgumentException("No sort mode with an ordinal of " + randomVal);
        };
    }

    private static StringDistanceImpl randomStringDistance() {
        int randomVal = randomIntBetween(0, 4);
        return switch (randomVal) {
            case 0 -> StringDistanceImpl.INTERNAL;
            case 1 -> StringDistanceImpl.DAMERAU_LEVENSHTEIN;
            case 2 -> StringDistanceImpl.LEVENSHTEIN;
            case 3 -> StringDistanceImpl.JARO_WINKLER;
            case 4 -> StringDistanceImpl.NGRAM;
            default -> throw new IllegalArgumentException("No string distance algorithm with an ordinal of " + randomVal);
        };
    }

    @Override
    protected void mutateSpecificParameters(TermSuggestionBuilder builder) throws IOException {
        switch (randomIntBetween(0, 9)) {
            case 0 -> builder.suggestMode(randomValueOtherThan(builder.suggestMode(), () -> randomSuggestMode()));
            case 1 -> builder.accuracy(randomValueOtherThan(builder.accuracy(), () -> randomFloat()));
            case 2 -> builder.sort(randomValueOtherThan(builder.sort(), () -> randomSort()));
            case 3 -> builder.stringDistance(randomValueOtherThan(builder.stringDistance(), () -> randomStringDistance()));
            case 4 -> builder.maxEdits(randomValueOtherThan(builder.maxEdits(), () -> randomIntBetween(1, 2)));
            case 5 -> builder.maxInspections(randomValueOtherThan(builder.maxInspections(), () -> randomInt(Integer.MAX_VALUE)));
            case 6 -> builder.maxTermFreq(randomValueOtherThan(builder.maxTermFreq(), () -> randomFloat()));
            case 7 -> builder.prefixLength(randomValueOtherThan(builder.prefixLength(), () -> randomInt(Integer.MAX_VALUE)));
            case 8 -> builder.minWordLength(randomValueOtherThan(builder.minWordLength(), () -> randomInt(Integer.MAX_VALUE)));
            case 9 -> builder.minDocFreq(randomValueOtherThan(builder.minDocFreq(), () -> randomFloat()));
        }
    }

    public void testInvalidParameters() {
        // test missing field name
        Exception e = expectThrows(NullPointerException.class, () -> new TermSuggestionBuilder((String) null));
        assertEquals("suggestion requires a field name", e.getMessage());

        // test empty field name
        e = expectThrows(IllegalArgumentException.class, () -> new TermSuggestionBuilder(""));
        assertEquals("suggestion field name is empty", e.getMessage());

        TermSuggestionBuilder builder = new TermSuggestionBuilder(randomAlphaOfLengthBetween(2, 20));

        // test invalid accuracy values
        expectThrows(IllegalArgumentException.class, () -> builder.accuracy(-0.5f));
        expectThrows(IllegalArgumentException.class, () -> builder.accuracy(1.1f));

        // test invalid max edit distance values
        expectThrows(IllegalArgumentException.class, () -> builder.maxEdits(0));
        expectThrows(IllegalArgumentException.class, () -> builder.maxEdits(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.maxEdits(3));

        // test invalid max inspections values
        expectThrows(IllegalArgumentException.class, () -> builder.maxInspections(-1));

        // test invalid max term freq values
        expectThrows(IllegalArgumentException.class, () -> builder.maxTermFreq(-0.5f));
        expectThrows(IllegalArgumentException.class, () -> builder.maxTermFreq(1.5f));
        builder.maxTermFreq(2.0f);

        // test invalid min doc freq values
        expectThrows(IllegalArgumentException.class, () -> builder.minDocFreq(-0.5f));
        expectThrows(IllegalArgumentException.class, () -> builder.minDocFreq(1.5f));
        builder.minDocFreq(2.0f);

        // test invalid min word length values
        expectThrows(IllegalArgumentException.class, () -> builder.minWordLength(0));
        expectThrows(IllegalArgumentException.class, () -> builder.minWordLength(-1));

        // test invalid prefix length values
        expectThrows(IllegalArgumentException.class, () -> builder.prefixLength(-1));

        // test invalid size values
        expectThrows(IllegalArgumentException.class, () -> builder.size(0));
        expectThrows(IllegalArgumentException.class, () -> builder.size(-1));

        // null values not allowed for enums
        expectThrows(NullPointerException.class, () -> builder.sort(null));
        expectThrows(NullPointerException.class, () -> builder.stringDistance(null));
        expectThrows(NullPointerException.class, () -> builder.suggestMode(null));
    }

    public void testDefaultValuesSet() {
        TermSuggestionBuilder builder = new TermSuggestionBuilder(randomAlphaOfLengthBetween(2, 20));
        assertEquals(DEFAULT_ACCURACY, builder.accuracy(), Float.MIN_VALUE);
        assertEquals(DEFAULT_MAX_EDITS, builder.maxEdits());
        assertEquals(DEFAULT_MAX_INSPECTIONS, builder.maxInspections());
        assertEquals(DEFAULT_MAX_TERM_FREQ, builder.maxTermFreq(), Float.MIN_VALUE);
        assertEquals(DEFAULT_MIN_DOC_FREQ, builder.minDocFreq(), Float.MIN_VALUE);
        assertEquals(DEFAULT_MIN_WORD_LENGTH, builder.minWordLength());
        assertEquals(DEFAULT_PREFIX_LENGTH, builder.prefixLength());
        assertEquals(SortBy.SCORE, builder.sort());
        assertEquals(StringDistanceImpl.INTERNAL, builder.stringDistance());
        assertEquals(SuggestMode.MISSING, builder.suggestMode());
    }

    public void testMalformedJson() {
        final String field = RandomStrings.randomAsciiOfLength(random(), 10).toLowerCase(Locale.ROOT);
        String suggest = Strings.format("""
            {
              "bad-payload" : {
                "text" : "the amsterdma meetpu",
                "term" : {
                  "field" : { "%s" : "bad-object" }
                }
              }
            }""", field);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, suggest)) {
            final SuggestBuilder suggestBuilder = SuggestBuilder.fromXContent(parser);
            fail("Should not have been able to create SuggestBuilder from malformed JSON: " + suggestBuilder);
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("parsing failed"));
        }
    }

    @Override
    protected void assertSuggestionContext(TermSuggestionBuilder builder, SuggestionContext context) {
        assertThat(context, instanceOf(TermSuggestionContext.class));
        assertThat(context.getSuggester(), instanceOf(TermSuggester.class));
        TermSuggestionContext termSuggesterCtx = (TermSuggestionContext) context;
        assertEquals(builder.accuracy(), termSuggesterCtx.getDirectSpellCheckerSettings().accuracy(), 0.0);
        assertEquals(builder.maxTermFreq(), termSuggesterCtx.getDirectSpellCheckerSettings().maxTermFreq(), 0.0);
        assertEquals(builder.minDocFreq(), termSuggesterCtx.getDirectSpellCheckerSettings().minDocFreq(), 0.0);
        assertEquals(builder.maxEdits(), termSuggesterCtx.getDirectSpellCheckerSettings().maxEdits());
        assertEquals(builder.maxInspections(), termSuggesterCtx.getDirectSpellCheckerSettings().maxInspections());
        assertEquals(builder.minWordLength(), termSuggesterCtx.getDirectSpellCheckerSettings().minWordLength());
        assertEquals(builder.prefixLength(), termSuggesterCtx.getDirectSpellCheckerSettings().prefixLength());
        assertEquals(builder.prefixLength(), termSuggesterCtx.getDirectSpellCheckerSettings().prefixLength());
        assertEquals(builder.suggestMode().toLucene(), termSuggesterCtx.getDirectSpellCheckerSettings().suggestMode());
        assertEquals(builder.sort(), termSuggesterCtx.getDirectSpellCheckerSettings().sort());
        // distance implementations don't implement equals() and have little to compare, so we only check class
        assertEquals(
            builder.stringDistance().toLucene().getClass(),
            termSuggesterCtx.getDirectSpellCheckerSettings().stringDistance().getClass()
        );
    }
}
