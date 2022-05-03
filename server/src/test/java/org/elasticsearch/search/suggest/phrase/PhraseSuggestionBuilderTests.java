/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class PhraseSuggestionBuilderTests extends AbstractSuggestionBuilderTestCase<PhraseSuggestionBuilder> {
    @Override
    protected PhraseSuggestionBuilder randomSuggestionBuilder() {
        return randomPhraseSuggestionBuilder();
    }

    public static PhraseSuggestionBuilder randomPhraseSuggestionBuilder() {
        PhraseSuggestionBuilder testBuilder = new PhraseSuggestionBuilder(randomAlphaOfLengthBetween(2, 20));
        setCommonPropertiesOnRandomBuilder(testBuilder);
        maybeSet(testBuilder::maxErrors, randomFloat());
        maybeSet(testBuilder::separator, randomAlphaOfLengthBetween(1, 10));
        maybeSet(testBuilder::realWordErrorLikelihood, randomFloat());
        maybeSet(testBuilder::confidence, randomFloat());
        maybeSet(testBuilder::collateQuery, randomAlphaOfLengthBetween(3, 20));
        // collate query prune and parameters will only be used when query is set
        if (testBuilder.collateQuery() != null) {
            maybeSet(testBuilder::collatePrune, randomBoolean());
            if (randomBoolean()) {
                Map<String, Object> collateParams = new HashMap<>();
                int numParams = randomIntBetween(1, 5);
                for (int i = 0; i < numParams; i++) {
                    collateParams.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
                }
                testBuilder.collateParams(collateParams);
            }
        }
        if (randomBoolean()) {
            // preTag, postTag
            testBuilder.highlight(randomAlphaOfLengthBetween(3, 20), randomAlphaOfLengthBetween(3, 20));
        }
        maybeSet(testBuilder::gramSize, randomIntBetween(1, 5));
        maybeSet(testBuilder::forceUnigrams, randomBoolean());
        maybeSet(testBuilder::tokenLimit, randomIntBetween(1, 20));
        if (randomBoolean()) {
            testBuilder.smoothingModel(randomSmoothingModel());
        }
        if (randomBoolean()) {
            int numGenerators = randomIntBetween(1, 5);
            for (int i = 0; i < numGenerators; i++) {
                testBuilder.addCandidateGenerator(DirectCandidateGeneratorTests.randomCandidateGenerator());
            }
        }
        return testBuilder;
    }

    private static SmoothingModel randomSmoothingModel() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> LaplaceModelTests.createRandomModel();
            case 1 -> StupidBackoffModelTests.createRandomModel();
            case 2 -> LinearInterpolationModelTests.createRandomModel();
            default -> null;
        };
    }

    @Override
    protected void mutateSpecificParameters(PhraseSuggestionBuilder builder) throws IOException {
        switch (randomIntBetween(0, 12)) {
            case 0 -> builder.maxErrors(randomValueOtherThan(builder.maxErrors(), () -> randomFloat()));
            case 1 -> builder.realWordErrorLikelihood(randomValueOtherThan(builder.realWordErrorLikelihood(), () -> randomFloat()));
            case 2 -> builder.confidence(randomValueOtherThan(builder.confidence(), () -> randomFloat()));
            case 3 -> builder.gramSize(randomValueOtherThan(builder.gramSize(), () -> randomIntBetween(1, 5)));
            case 4 -> builder.tokenLimit(randomValueOtherThan(builder.tokenLimit(), () -> randomIntBetween(1, 20)));
            case 5 -> builder.separator(randomValueOtherThan(builder.separator(), () -> randomAlphaOfLengthBetween(1, 10)));
            case 6 -> {
                Script collateQuery = builder.collateQuery();
                if (collateQuery != null) {
                    builder.collateQuery(randomValueOtherThan(collateQuery.getIdOrCode(), () -> randomAlphaOfLengthBetween(3, 20)));
                } else {
                    builder.collateQuery(randomAlphaOfLengthBetween(3, 20));
                }
            }
            case 7 -> builder.collatePrune(builder.collatePrune() == null ? randomBoolean() : builder.collatePrune() == false);
            case 8 -> {
                // preTag, postTag
                String currentPre = builder.preTag();
                if (currentPre != null) {
                    // simply double both values
                    builder.highlight(builder.preTag() + builder.preTag(), builder.postTag() + builder.postTag());
                } else {
                    builder.highlight(randomAlphaOfLengthBetween(3, 20), randomAlphaOfLengthBetween(3, 20));
                }
            }
            case 9 -> builder.forceUnigrams(builder.forceUnigrams() == null ? randomBoolean() : builder.forceUnigrams() == false);
            case 10 -> {
                Map<String, Object> collateParams = builder.collateParams() == null
                    ? Maps.newMapWithExpectedSize(1)
                    : builder.collateParams();
                collateParams.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
                builder.collateParams(collateParams);
            }
            case 11 -> builder.smoothingModel(
                randomValueOtherThan(builder.smoothingModel(), PhraseSuggestionBuilderTests::randomSmoothingModel)
            );
            case 12 -> builder.addCandidateGenerator(DirectCandidateGeneratorTests.randomCandidateGenerator());
        }
    }

    public void testInvalidParameters() {
        // test missing field name
        Exception e = expectThrows(NullPointerException.class, () -> new PhraseSuggestionBuilder((String) null));
        assertEquals("suggestion requires a field name", e.getMessage());

        // test empty field name
        e = expectThrows(IllegalArgumentException.class, () -> new PhraseSuggestionBuilder(""));
        assertEquals("suggestion field name is empty", e.getMessage());

        PhraseSuggestionBuilder builder = new PhraseSuggestionBuilder(randomAlphaOfLengthBetween(2, 20));

        e = expectThrows(IllegalArgumentException.class, () -> builder.gramSize(0));
        assertEquals("gramSize must be >= 1", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> builder.gramSize(-1));
        assertEquals("gramSize must be >= 1", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> builder.maxErrors(-1));
        assertEquals("max_error must be > 0.0", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> builder.separator(null));
        assertEquals("separator cannot be set to null", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> builder.realWordErrorLikelihood(-1));
        assertEquals("real_word_error_likelihood must be > 0.0", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> builder.confidence(-1));
        assertEquals("confidence must be >= 0.0", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> builder.tokenLimit(0));
        assertEquals("token_limit must be >= 1", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> builder.highlight(null, "</b>"));
        assertEquals("Pre and post tag must both be null or both not be null.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> builder.highlight("<b>", null));
        assertEquals("Pre and post tag must both be null or both not be null.", e.getMessage());
    }

    @Override
    protected void assertSuggestionContext(PhraseSuggestionBuilder builder, SuggestionContext context) {
        assertThat(context, instanceOf(PhraseSuggestionContext.class));
        assertThat(context.getSuggester(), instanceOf(PhraseSuggester.class));
        PhraseSuggestionContext phraseSuggesterCtx = (PhraseSuggestionContext) context;
        assertOptionalEquals(builder.confidence(), phraseSuggesterCtx.confidence(), PhraseSuggestionContext.DEFAULT_CONFIDENCE);
        assertOptionalEquals(builder.collatePrune(), phraseSuggesterCtx.collatePrune(), PhraseSuggestionContext.DEFAULT_COLLATE_PRUNE);
        assertEquals(builder.separator(), phraseSuggesterCtx.separator().utf8ToString());
        assertOptionalEquals(
            builder.realWordErrorLikelihood(),
            phraseSuggesterCtx.realworldErrorLikelihood(),
            PhraseSuggestionContext.DEFAULT_RWE_ERRORLIKELIHOOD
        );
        assertOptionalEquals(builder.maxErrors(), phraseSuggesterCtx.maxErrors(), PhraseSuggestionContext.DEFAULT_MAX_ERRORS);
        assertOptionalEquals(
            builder.forceUnigrams(),
            phraseSuggesterCtx.getRequireUnigram(),
            PhraseSuggestionContext.DEFAULT_REQUIRE_UNIGRAM
        );
        assertOptionalEquals(builder.tokenLimit(), phraseSuggesterCtx.getTokenLimit(), NoisyChannelSpellChecker.DEFAULT_TOKEN_LIMIT);
        assertEquals(builder.preTag(), phraseSuggesterCtx.getPreTag() != null ? phraseSuggesterCtx.getPreTag().utf8ToString() : null);
        assertEquals(builder.postTag(), phraseSuggesterCtx.getPostTag() != null ? phraseSuggesterCtx.getPostTag().utf8ToString() : null);
        assertOptionalEquals(builder.gramSize(), phraseSuggesterCtx.gramSize(), PhraseSuggestionContext.DEFAULT_GRAM_SIZE);
        if (builder.collateQuery() != null) {
            assertEquals(builder.collateQuery().getIdOrCode(), phraseSuggesterCtx.getCollateQueryScript().newInstance(null).execute());
        }
        if (builder.collateParams() != null) {
            assertEquals(builder.collateParams(), phraseSuggesterCtx.getCollateScriptParams());
        }
        if (builder.smoothingModel() != null) {
            assertEquals(builder.smoothingModel().buildWordScorerFactory().getClass(), phraseSuggesterCtx.model().getClass());
        }
        if (builder.getCandidateGenerators().isEmpty() == false) {
            // currently, "direct_generator" is the only one available. Only compare size of the lists
            assertEquals(builder.getCandidateGenerators().get("direct_generator").size(), phraseSuggesterCtx.generators().size());
        }
    }

    private static <T> void assertOptionalEquals(T optional, T actual, T defaultValue) {
        if (optional != null) {
            assertEquals(optional, actual);
        } else {
            assertEquals(defaultValue, actual);
        }
    }
}
