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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;
import org.elasticsearch.search.suggest.completion.context.GeoContextMapping;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;
import org.elasticsearch.search.suggest.completion.context.QueryContext;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class CompletionSuggesterBuilderTests extends AbstractSuggestionBuilderTestCase<CompletionSuggestionBuilder> {

    @Override
    protected CompletionSuggestionBuilder randomSuggestionBuilder() {
        return randomSuggestionBuilderWithContextInfo().builder;
    }

    private static class BuilderAndInfo {
        CompletionSuggestionBuilder builder;
        List<String> catContexts = new ArrayList<>();
        List<String> geoContexts = new ArrayList<>();
    }

    private BuilderAndInfo randomSuggestionBuilderWithContextInfo() {
        final BuilderAndInfo builderAndInfo = new BuilderAndInfo();
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
        Map<String, List<? extends QueryContext>> contextMap = new HashMap<>();
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

    @Override
    protected void assertSuggestionContext(SuggestionContext oldSuggestion, SuggestionContext newSuggestion) {
        assertThat(oldSuggestion, instanceOf(CompletionSuggestionContext.class));
        assertThat(newSuggestion, instanceOf(CompletionSuggestionContext.class));
        CompletionSuggestionContext oldCompletionSuggestion = (CompletionSuggestionContext) oldSuggestion;
        CompletionSuggestionContext newCompletionSuggestion = (CompletionSuggestionContext) newSuggestion;
        assertEquals(oldCompletionSuggestion.getPayloadFields(), newCompletionSuggestion.getPayloadFields());
        assertEquals(oldCompletionSuggestion.getFuzzyOptions(), newCompletionSuggestion.getFuzzyOptions());
        assertEquals(oldCompletionSuggestion.getRegexOptions(), newCompletionSuggestion.getRegexOptions());
        assertEquals(oldCompletionSuggestion.getQueryContexts(), newCompletionSuggestion.getQueryContexts());

    }

    @Override
    protected Tuple<MapperService, CompletionSuggestionBuilder> mockMapperServiceAndSuggestionBuilder(
        IndexSettings idxSettings, AnalysisService mockAnalysisService, CompletionSuggestionBuilder suggestBuilder) {
        final BuilderAndInfo builderAndInfo = randomSuggestionBuilderWithContextInfo();
        final MapperService mapperService = new MapperService(idxSettings, mockAnalysisService, null,
            new IndicesModule().getMapperRegistry(), null) {
            @Override
            public MappedFieldType fullName(String fullName) {
                CompletionFieldMapper.CompletionFieldType type = new CompletionFieldMapper.CompletionFieldType();
                List<ContextMapping> contextMappings = builderAndInfo.catContexts.stream()
                    .map(catContext -> new CategoryContextMapping.Builder(catContext).build())
                    .collect(Collectors.toList());
                contextMappings.addAll(builderAndInfo.geoContexts.stream()
                    .map(geoContext -> new GeoContextMapping.Builder(geoContext).build())
                    .collect(Collectors.toList()));
                type.setContextMappings(new ContextMappings(contextMappings));
                return type;
            }
        };
        final CompletionSuggestionBuilder builder = builderAndInfo.builder;
        return new Tuple<>(mapperService, builder);
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
                int nCatContext = randomIntBetween(1, 5);
                List<CategoryQueryContext> contexts = new ArrayList<>(nCatContext);
                for (int i = 0; i < nCatContext; i++) {
                    contexts.add(CategoryQueryContextTests.randomCategoryQueryContext());
                }
                builder.contexts(Collections.singletonMap(randomAsciiOfLength(10), contexts));
                break;
            case 2:
                int nGeoContext = randomIntBetween(1, 5);
                List<GeoQueryContext> geoContexts = new ArrayList<>(nGeoContext);
                for (int i = 0; i < nGeoContext; i++) {
                    geoContexts.add(GeoQueryContextTests.randomGeoQueryContext());
                }
                builder.contexts(Collections.singletonMap(randomAsciiOfLength(10), geoContexts));
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
