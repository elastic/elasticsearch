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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompletionSuggesterBuilderTests extends AbstractSuggestionBuilderTestCase<CompletionSuggestionBuilder> {

    private static final String[] SHUFFLE_PROTECTED_FIELDS = new String[] {CompletionSuggestionBuilder.CONTEXTS_FIELD.getPreferredName()};

    @Override
    protected CompletionSuggestionBuilder randomSuggestionBuilder() {
        return randomCompletionSuggestionBuilder();
    }

    public static CompletionSuggestionBuilder randomCompletionSuggestionBuilder() {
        return randomSuggestionBuilderWithContextInfo().builder;
    }

    private static class BuilderAndInfo {
        CompletionSuggestionBuilder builder;
        List<String> catContexts = new ArrayList<>();
        List<String> geoContexts = new ArrayList<>();
    }

    private static BuilderAndInfo randomSuggestionBuilderWithContextInfo() {
        final BuilderAndInfo builderAndInfo = new BuilderAndInfo();
        CompletionSuggestionBuilder testBuilder = new CompletionSuggestionBuilder(randomAsciiOfLengthBetween(2, 20));
        setCommonPropertiesOnRandomBuilder(testBuilder);
        switch (randomIntBetween(0, 3)) {
            case 0:
                testBuilder.prefix(randomAsciiOfLength(10));
                break;
            case 1:
                testBuilder.prefix(randomAsciiOfLength(10), FuzzyOptionsTests.randomFuzzyOptions());
                break;
            case 2:
                testBuilder.prefix(randomAsciiOfLength(10), randomFrom(Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO));
                break;
            case 3:
                testBuilder.regex(randomAsciiOfLength(10), RegexOptionsTests.randomRegexOptions());
                break;
        }
        Map<String, List<? extends ToXContent>> contextMap = new HashMap<>();
        if (randomBoolean()) {
            int numContext = randomIntBetween(1, 5);
            List<CategoryQueryContext> contexts = new ArrayList<>(numContext);
            for (int i = 0; i < numContext; i++) {
                contexts.add(CategoryQueryContextTests.randomCategoryQueryContext());
            }
            String name = randomAsciiOfLength(10);
            contextMap.put(name, contexts);
            builderAndInfo.catContexts.add(name);
        }
        if (randomBoolean()) {
            int numContext = randomIntBetween(1, 5);
            List<GeoQueryContext> contexts = new ArrayList<>(numContext);
            for (int i = 0; i < numContext; i++) {
                contexts.add(GeoQueryContextTests.randomGeoQueryContext());
            }
            String name = randomAsciiOfLength(10);
            contextMap.put(name, contexts);
            builderAndInfo.geoContexts.add(name);
        }
        testBuilder.contexts(contextMap);
        builderAndInfo.builder = testBuilder;
        return builderAndInfo;
    }

    /**
     * exclude the "contexts" field from recursive random shuffling in fromXContent tests or else
     * the equals() test will fail because their {@link BytesReference} representation isn't the same
     */
    @Override
    protected String[] shuffleProtectedFields() {
        return SHUFFLE_PROTECTED_FIELDS;
    }

    @Override
    protected void mutateSpecificParameters(CompletionSuggestionBuilder builder) throws IOException {
        switch (randomIntBetween(0, 4)) {
            case 0:
                int nCatContext = randomIntBetween(1, 5);
                List<CategoryQueryContext> contexts = new ArrayList<>(nCatContext);
                for (int i = 0; i < nCatContext; i++) {
                    contexts.add(CategoryQueryContextTests.randomCategoryQueryContext());
                }
                builder.contexts(Collections.singletonMap(randomAsciiOfLength(10), contexts));
                break;
            case 1:
                int nGeoContext = randomIntBetween(1, 5);
                List<GeoQueryContext> geoContexts = new ArrayList<>(nGeoContext);
                for (int i = 0; i < nGeoContext; i++) {
                    geoContexts.add(GeoQueryContextTests.randomGeoQueryContext());
                }
                builder.contexts(Collections.singletonMap(randomAsciiOfLength(10), geoContexts));
                break;
            case 2:
                builder.prefix(randomAsciiOfLength(10), FuzzyOptionsTests.randomFuzzyOptions());
                break;
            case 3:
                builder.prefix(randomAsciiOfLength(10), randomFrom(Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO));
                break;
            case 4:
                builder.regex(randomAsciiOfLength(10), RegexOptionsTests.randomRegexOptions());
                break;
            default:
                throw new IllegalStateException("should not through");
        }
    }
}
