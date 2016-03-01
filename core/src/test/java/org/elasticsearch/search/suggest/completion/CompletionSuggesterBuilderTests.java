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

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;
import org.elasticsearch.search.suggest.completion.context.QueryContext;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CompletionSuggesterBuilderTests extends AbstractSuggestionBuilderTestCase<CompletionSuggestionBuilder> {

    @BeforeClass
    public static void initQueryContexts() {
        namedWriteableRegistry.registerPrototype(QueryContext.class, CategoryQueryContext.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(QueryContext.class, GeoQueryContext.PROTOTYPE);
    }

    @Override
    protected CompletionSuggestionBuilder randomSuggestionBuilder() {
        CompletionSuggestionBuilder testBuilder = new CompletionSuggestionBuilder(randomAsciiOfLengthBetween(2, 20));
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
        List<String> payloads = new ArrayList<>();
        Collections.addAll(payloads, generateRandomStringArray(5, 10, false, false));
        maybeSet(testBuilder::payload, payloads);
        if (randomBoolean()) {
            int numContext = randomIntBetween(1, 5);
            CategoryQueryContext[] contexts = new CategoryQueryContext[numContext];
            for (int i = 0; i < numContext; i++) {
                contexts[i] = CategoryQueryContextTests.randomCategoryQueryContext();
            }
            testBuilder.categoryContexts(randomAsciiOfLength(10), contexts);
        }
        if (randomBoolean()) {
            int numContext = randomIntBetween(1, 5);
            GeoQueryContext[] contexts = new GeoQueryContext[numContext];
            for (int i = 0; i < numContext; i++) {
                contexts[i] = GeoQueryContextTests.randomGeoQueryContext();
            }
            testBuilder.geoContexts(randomAsciiOfLength(10), contexts);
        }
        return testBuilder;
    }

    @Override
    protected void assertSuggestionContext(SuggestionContext oldSuggestion, SuggestionContext newSuggestion) {

    }

    @Override
    public void testBuild() throws IOException {
        // skip for now
    }

    @Override
    public void testFromXContent() throws IOException {
        // skip for now
    }

    @Override
    protected void mutateSpecificParameters(CompletionSuggestionBuilder builder) throws IOException {
        switch (randomIntBetween(0, 5)) {
            case 0:
                List<String> payloads = new ArrayList<>();
                Collections.addAll(payloads, generateRandomStringArray(5, 10, false, false));
                builder.payload(payloads);
                break;
            case 1:
                int numCategoryContext = randomIntBetween(1, 5);
                CategoryQueryContext[] categoryContexts = new CategoryQueryContext[numCategoryContext];
                for (int i = 0; i < numCategoryContext; i++) {
                    categoryContexts[i] = CategoryQueryContextTests.randomCategoryQueryContext();
                }
                builder.categoryContexts(randomAsciiOfLength(10), categoryContexts);
                break;
            case 2:
                int numGeoContext = randomIntBetween(1, 5);
                GeoQueryContext[] geoContexts = new GeoQueryContext[numGeoContext];
                for (int i = 0; i < numGeoContext; i++) {
                    geoContexts[i] = GeoQueryContextTests.randomGeoQueryContext();
                }
                builder.geoContexts(randomAsciiOfLength(10), geoContexts);
                break;
            case 3:
                builder.prefix(randomAsciiOfLength(10), FuzzyOptionsTests.randomFuzzyOptions());
                break;
            case 4:
                builder.prefix(randomAsciiOfLength(10), randomFrom(Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO));
                break;
            case 5:
                builder.regex(randomAsciiOfLength(10), RegexOptionsTests.randomRegexOptions());
                break;
            default:
                throw new IllegalStateException("should not through");
        }
    }
}
