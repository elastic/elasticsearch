/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class LinearRetrieverBuilderParsingTests extends AbstractXContentTestCase<LinearRetrieverBuilder> {
    private static List<NamedXContentRegistry.Entry> xContentRegistryEntries;

    @BeforeClass
    public static void init() {
        xContentRegistryEntries = new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        xContentRegistryEntries = null;
    }

    /**
     * Creates a random {@link LinearRetrieverBuilder}. The created instance is not guaranteed to pass {@link SearchRequest} validation.
     * This is purely for x-content testing.
     */
    @Override
    protected LinearRetrieverBuilder createTestInstance() {
        int rankWindowSize = randomInt(100);

        List<String> fields = null;
        String query = null;
        ScoreNormalizer normalizer = null;
        if (randomBoolean()) {
            fields = randomList(1, 10, () -> randomAlphaOfLengthBetween(1, 10));
            query = randomAlphaOfLengthBetween(1, 10);
            normalizer = randomScoreNormalizer();
        }

        int num = randomIntBetween(1, 3);
        List<CompoundRetrieverBuilder.RetrieverSource> innerRetrievers = new ArrayList<>();
        float[] weights = new float[num];
        ScoreNormalizer[] normalizers = new ScoreNormalizer[num];
        for (int i = 0; i < num; i++) {
            innerRetrievers.add(
                new CompoundRetrieverBuilder.RetrieverSource(TestRetrieverBuilder.createRandomTestRetrieverBuilder(), null)
            );
            weights[i] = randomFloat();
            if (randomBoolean()) {
                normalizers[i] = randomScoreNormalizer();
            } else {
                normalizers[i] = null;
            }
        }

        return new LinearRetrieverBuilder(innerRetrievers, fields, query, normalizer, rankWindowSize, weights, normalizers);
    }

    @Override
    protected LinearRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (LinearRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
            parser,
            new RetrieverParserContext(new SearchUsage(), n -> true)
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(xContentRegistryEntries);
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                TestRetrieverBuilder.TEST_SPEC.getName(),
                (p, c) -> TestRetrieverBuilder.TEST_SPEC.getParser().fromXContent(p, (RetrieverParserContext) c),
                TestRetrieverBuilder.TEST_SPEC.getName().getForRestApiVersion()
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                new ParseField(LinearRetrieverBuilder.NAME),
                (p, c) -> LinearRetrieverBuilder.PARSER.apply(p, (RetrieverParserContext) c)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    private static ScoreNormalizer randomScoreNormalizer() {
        int random = randomInt(2);
        return switch (random) {
            case 0 -> MinMaxScoreNormalizer.INSTANCE;
            case 1 -> L2ScoreNormalizer.INSTANCE;
            default -> IdentityScoreNormalizer.INSTANCE;
        };
    }

    public void testTopLevelNormalizer() throws IOException {
        String json = """
            {
              "retrievers": [
                { "test_retriever": {} },
                { "test_retriever": {} }
              ],
              "normalizer": "min_max"
            }""";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {

            LinearRetrieverBuilder builder = doParseInstance(parser);
            assertThat(builder.getNormalizer(), instanceOf(MinMaxScoreNormalizer.class));
            for (ScoreNormalizer normalizer : builder.getNormalizers()) {
                assertThat(normalizer, instanceOf(MinMaxScoreNormalizer.class));
            }
        }
    }
}
