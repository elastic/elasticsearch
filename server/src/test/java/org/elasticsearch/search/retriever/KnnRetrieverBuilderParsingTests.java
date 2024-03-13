/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;

public class KnnRetrieverBuilderParsingTests extends AbstractXContentTestCase<KnnRetrieverBuilder> {

    /**
     * Creates a random {@link KnnRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static KnnRetrieverBuilder createRandomKnnRetrieverBuilder() {
        String field = randomAlphaOfLength(6);
        int dim = randomIntBetween(2, 30);
        float[] vector = randomBoolean() ? null : randomVector(dim);
        int k = randomIntBetween(1, 100);
        int numCands = randomIntBetween(k + 20, 1000);
        Float similarity = randomBoolean() ? null : randomFloat();

        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(field, vector, null, k, numCands, similarity);

        List<QueryBuilder> preFilterQueryBuilders = new ArrayList<>();

        if (randomBoolean()) {
            for (int i = 0; i < randomIntBetween(1, 3); ++i) {
                preFilterQueryBuilders.add(RandomQueryBuilder.createQuery(random()));
            }
        }

        knnRetrieverBuilder.preFilterQueryBuilders.addAll(preFilterQueryBuilders);

        return knnRetrieverBuilder;
    }

    @Override
    protected KnnRetrieverBuilder createTestInstance() {
        return createRandomKnnRetrieverBuilder();
    }

    @Override
    protected KnnRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return KnnRetrieverBuilder.fromXContent(
            parser,
            new RetrieverParserContext(
                new SearchUsage(),
                nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED || nf == KnnRetrieverBuilder.KNN_RETRIEVER_SUPPORTED
            )
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }
}
