/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.CompletionFieldMapper.CompletionFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMapping.InternalQueryContext;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class CompletionSuggesterBuilderTests extends AbstractSuggestionBuilderTestCase<CompletionSuggestionBuilder> {

    private static final String[] SHUFFLE_PROTECTED_FIELDS = new String[] { CompletionSuggestionBuilder.CONTEXTS_FIELD.getPreferredName() };
    private static String categoryContextName;
    private static String geoQueryContextName;
    private static List<ContextMapping<?>> contextMappings = new ArrayList<>();

    @Override
    protected CompletionSuggestionBuilder randomSuggestionBuilder() {
        return randomCompletionSuggestionBuilder();
    }

    public static CompletionSuggestionBuilder randomCompletionSuggestionBuilder() {
        // lazy initialization of context names and mappings, cannot be done in some init method because other test
        // also create random CompletionSuggestionBuilder instances
        if (categoryContextName == null) {
            categoryContextName = randomAlphaOfLength(10);
        }
        if (geoQueryContextName == null) {
            geoQueryContextName = randomAlphaOfLength(10);
        }
        if (contextMappings.isEmpty()) {
            contextMappings.add(ContextBuilder.category(categoryContextName).build());
            contextMappings.add(ContextBuilder.geo(geoQueryContextName).build());
        }
        // lazy initialization of context names and mappings, cannot be done in some init method because other test
        // also create random CompletionSuggestionBuilder instances
        if (categoryContextName == null) {
            categoryContextName = randomAlphaOfLength(10);
        }
        if (geoQueryContextName == null) {
            geoQueryContextName = randomAlphaOfLength(10);
        }
        if (contextMappings.isEmpty()) {
            contextMappings.add(ContextBuilder.category(categoryContextName).build());
            contextMappings.add(ContextBuilder.geo(geoQueryContextName).build());
        }
        CompletionSuggestionBuilder testBuilder = new CompletionSuggestionBuilder(randomAlphaOfLengthBetween(2, 20));
        setCommonPropertiesOnRandomBuilder(testBuilder);
        switch (randomIntBetween(0, 3)) {
            case 0:
                testBuilder.prefix(randomAlphaOfLength(10));
                break;
            case 1:
                testBuilder.prefix(randomAlphaOfLength(10), FuzzyOptionsTests.randomFuzzyOptions());
                break;
            case 2:
                testBuilder.prefix(randomAlphaOfLength(10), randomFrom(Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO));
                break;
            case 3:
                testBuilder.regex(randomAlphaOfLength(10), RegexOptionsTests.randomRegexOptions());
                break;
        }
        Map<String, List<? extends ToXContent>> contextMap = new HashMap<>();
        if (randomBoolean()) {
            int numContext = randomIntBetween(1, 5);
            List<CategoryQueryContext> contexts = new ArrayList<>(numContext);
            for (int i = 0; i < numContext; i++) {
                contexts.add(CategoryQueryContextTests.randomCategoryQueryContext());
            }
            contextMap.put(categoryContextName, contexts);
        }
        if (randomBoolean()) {
            int numContext = randomIntBetween(1, 5);
            List<GeoQueryContext> contexts = new ArrayList<>(numContext);
            for (int i = 0; i < numContext; i++) {
                contexts.add(GeoQueryContextTests.randomGeoQueryContext());
            }
            contextMap.put(geoQueryContextName, contexts);
        }
        testBuilder.contexts(contextMap);
        testBuilder.skipDuplicates(randomBoolean());
        return testBuilder;
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
        switch (randomIntBetween(0, 5)) {
            case 0:
                int nCatContext = randomIntBetween(1, 5);
                List<CategoryQueryContext> contexts = new ArrayList<>(nCatContext);
                for (int i = 0; i < nCatContext; i++) {
                    contexts.add(CategoryQueryContextTests.randomCategoryQueryContext());
                }
                builder.contexts(Collections.singletonMap(randomAlphaOfLength(10), contexts));
                break;
            case 1:
                int nGeoContext = randomIntBetween(1, 5);
                List<GeoQueryContext> geoContexts = new ArrayList<>(nGeoContext);
                for (int i = 0; i < nGeoContext; i++) {
                    geoContexts.add(GeoQueryContextTests.randomGeoQueryContext());
                }
                builder.contexts(Collections.singletonMap(randomAlphaOfLength(10), geoContexts));
                break;
            case 2:
                builder.prefix(randomAlphaOfLength(10), FuzzyOptionsTests.randomFuzzyOptions());
                break;
            case 3:
                builder.prefix(randomAlphaOfLength(10), randomFrom(Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO));
                break;
            case 4:
                builder.regex(randomAlphaOfLength(10), RegexOptionsTests.randomRegexOptions());
                break;
            case 5:
                builder.skipDuplicates(builder.skipDuplicates == false);
                break;
            default:
                throw new IllegalStateException("should not through");
        }
    }

    @Override
    protected MappedFieldType mockFieldType(String fieldName) {
        CompletionFieldType completionFieldType = new CompletionFieldType(fieldName,
            new NamedAnalyzer("fieldSearchAnalyzer", AnalyzerScope.INDEX, new SimpleAnalyzer()),
            Collections.emptyMap());
        completionFieldType.setContextMappings(new ContextMappings(contextMappings));
        return completionFieldType;
    }

    @Override
    protected void assertSuggestionContext(CompletionSuggestionBuilder builder, SuggestionContext context) throws IOException {
        assertThat(context, instanceOf(CompletionSuggestionContext.class));
        assertThat(context.getSuggester(), instanceOf(CompletionSuggester.class));
        CompletionSuggestionContext completionSuggestionCtx = (CompletionSuggestionContext) context;
        assertThat(completionSuggestionCtx.getFieldType(), instanceOf(CompletionFieldType.class) );
        assertEquals(builder.fuzzyOptions, completionSuggestionCtx.getFuzzyOptions());
        Map<String, List<InternalQueryContext>> parsedContextBytes;
        parsedContextBytes = CompletionSuggestionBuilder.parseContextBytes(builder.contextBytes, xContentRegistry(),
                new ContextMappings(contextMappings));
        Map<String, List<InternalQueryContext>> queryContexts = completionSuggestionCtx.getQueryContexts();
        assertEquals(parsedContextBytes.keySet(), queryContexts.keySet());
        for (String contextName : queryContexts.keySet()) {
            assertEquals(parsedContextBytes.get(contextName), queryContexts.get(contextName));
        }
        assertEquals(builder.regexOptions, completionSuggestionCtx.getRegexOptions());
        assertEquals(builder.skipDuplicates, completionSuggestionCtx.isSkipDuplicates());
    }
}
