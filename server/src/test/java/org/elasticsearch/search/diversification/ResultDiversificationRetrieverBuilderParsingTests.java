/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class ResultDiversificationRetrieverBuilderParsingTests extends AbstractXContentTestCase<ResultDiversificationRetrieverBuilder> {
    private List<NamedXContentRegistry.Entry> xContentRegistryEntries;

    @Before
    public void beforeTest() {
        xContentRegistryEntries = new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents();
    }

    @After
    public void afterTest() throws Exception {
        xContentRegistryEntries = null;
    }

    @Override
    protected ResultDiversificationRetrieverBuilder createTestInstance() {
        int rankWindowSize = randomIntBetween(1, 20);
        float[] queryVector = randomBoolean() ? getRandomQueryVector() : null;
        Float lambda = randomBoolean() ? randomFloatBetween(0.0f, 1.0f, true) : null;
        CompoundRetrieverBuilder.RetrieverSource innerRetriever = new CompoundRetrieverBuilder.RetrieverSource(
            TestRetrieverBuilder.createRandomTestRetrieverBuilder(),
            null
        );
        return new ResultDiversificationRetrieverBuilder(
            innerRetriever,
            ResultDiversificationType.MMR,
            "test_field",
            rankWindowSize,
            queryVector,
            lambda
        );
    }

    @Override
    protected ResultDiversificationRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (ResultDiversificationRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
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
        return new NamedXContentRegistry(entries);
    }

    private float[] getRandomQueryVector() {
        float[] queryVector = new float[randomIntBetween(5, 256)];
        for (int i = 0; i < queryVector.length; i++) {
            queryVector[i] = randomFloatBetween(0.0f, 1.0f, true);
        }
        return queryVector;
    }
}
