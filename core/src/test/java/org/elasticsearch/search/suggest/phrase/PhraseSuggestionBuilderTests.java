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

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.script.Template;
import org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PhraseSuggestionBuilderTests extends AbstractSuggestionBuilderTestCase<PhraseSuggestionBuilder> {
    @Override
    protected PhraseSuggestionBuilder randomSuggestionBuilder() {
        return randomPhraseSuggestionBuilder();
    }

    public static PhraseSuggestionBuilder randomPhraseSuggestionBuilder() {
        PhraseSuggestionBuilder testBuilder = new PhraseSuggestionBuilder(randomAsciiOfLengthBetween(2, 20));
        setCommonPropertiesOnRandomBuilder(testBuilder);
        maybeSet(testBuilder::maxErrors, randomFloat());
        maybeSet(testBuilder::separator, randomAsciiOfLengthBetween(1, 10));
        maybeSet(testBuilder::realWordErrorLikelihood, randomFloat());
        maybeSet(testBuilder::confidence, randomFloat());
        maybeSet(testBuilder::collateQuery, randomAsciiOfLengthBetween(3, 20));
        // collate query prune and parameters will only be used when query is set
        if (testBuilder.collateQuery() != null) {
            maybeSet(testBuilder::collatePrune, randomBoolean());
            if (randomBoolean()) {
                Map<String, Object> collateParams = new HashMap<>();
                int numParams = randomIntBetween(1, 5);
                for (int i = 0; i < numParams; i++) {
                    collateParams.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
                }
                testBuilder.collateParams(collateParams );
            }
        }
        if (randomBoolean()) {
            // preTag, postTag
            testBuilder.highlight(randomAsciiOfLengthBetween(3, 20), randomAsciiOfLengthBetween(3, 20));
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
        SmoothingModel model = null;
        switch (randomIntBetween(0,2)) {
        case 0:
            model = LaplaceModelTests.createRandomModel();
            break;
        case 1:
            model = StupidBackoffModelTests.createRandomModel();
            break;
        case 2:
            model = LinearInterpolationModelTests.createRandomModel();
            break;
        }
        return model;
    }

    @Override
    protected void mutateSpecificParameters(PhraseSuggestionBuilder builder) throws IOException {
        switch (randomIntBetween(0, 12)) {
        case 0:
            builder.maxErrors(randomValueOtherThan(builder.maxErrors(), () -> randomFloat()));
            break;
        case 1:
            builder.realWordErrorLikelihood(randomValueOtherThan(builder.realWordErrorLikelihood(), () -> randomFloat()));
            break;
        case 2:
            builder.confidence(randomValueOtherThan(builder.confidence(), () -> randomFloat()));
            break;
        case 3:
            builder.gramSize(randomValueOtherThan(builder.gramSize(), () -> randomIntBetween(1, 5)));
            break;
        case 4:
            builder.tokenLimit(randomValueOtherThan(builder.tokenLimit(), () -> randomIntBetween(1, 20)));
            break;
        case 5:
            builder.separator(randomValueOtherThan(builder.separator(), () -> randomAsciiOfLengthBetween(1, 10)));
            break;
        case 6:
            Template collateQuery = builder.collateQuery();
            if (collateQuery != null) {
                builder.collateQuery(randomValueOtherThan(collateQuery.getScript(), () -> randomAsciiOfLengthBetween(3, 20)));
            } else {
                builder.collateQuery(randomAsciiOfLengthBetween(3, 20));
            }
            break;
        case 7:
            builder.collatePrune(builder.collatePrune() == null ? randomBoolean() : !builder.collatePrune() );
            break;
        case 8:
            // preTag, postTag
            String currentPre = builder.preTag();
            if (currentPre != null) {
                // simply double both values
                builder.highlight(builder.preTag() + builder.preTag(), builder.postTag() + builder.postTag());
            } else {
                builder.highlight(randomAsciiOfLengthBetween(3, 20), randomAsciiOfLengthBetween(3, 20));
            }
            break;
        case 9:
            builder.forceUnigrams(builder.forceUnigrams() == null ? randomBoolean() : ! builder.forceUnigrams());
            break;
        case 10:
            Map<String, Object> collateParams = builder.collateParams() == null ? new HashMap<>(1) : builder.collateParams();
            collateParams.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
            builder.collateParams(collateParams);
            break;
        case 11:
            builder.smoothingModel(randomValueOtherThan(builder.smoothingModel(), PhraseSuggestionBuilderTests::randomSmoothingModel));
            break;
        case 12:
            builder.addCandidateGenerator(DirectCandidateGeneratorTests.randomCandidateGenerator());
            break;
        }
    }

    public void testInvalidParameters() throws IOException {
        // test missing field name
        Exception e = expectThrows(NullPointerException.class, () -> new PhraseSuggestionBuilder((String) null));
        assertEquals("suggestion requires a field name", e.getMessage());

        // test empty field name
        e = expectThrows(IllegalArgumentException.class, () -> new PhraseSuggestionBuilder(""));
        assertEquals("suggestion field name is empty", e.getMessage());

        PhraseSuggestionBuilder builder = new PhraseSuggestionBuilder(randomAsciiOfLengthBetween(2, 20));
 
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

}
