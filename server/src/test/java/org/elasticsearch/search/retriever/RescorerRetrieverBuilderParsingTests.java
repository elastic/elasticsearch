/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.rescore.QueryRescorerBuilderTests;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class RescorerRetrieverBuilderParsingTests extends AbstractXContentTestCase<RescorerRetrieverBuilder> {
    private static List<NamedXContentRegistry.Entry> xContentRegistryEntries;

    @BeforeClass
    public static void init() {
        xContentRegistryEntries = new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        xContentRegistryEntries = null;
    }

    @Override
    protected RescorerRetrieverBuilder createTestInstance() {
        int num = randomIntBetween(1, 3);
        List<RescorerBuilder<?>> rescorers = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            rescorers.add(QueryRescorerBuilderTests.randomRescoreBuilder());
        }
        return new RescorerRetrieverBuilder(TestRetrieverBuilder.createRandomTestRetrieverBuilder(), rescorers);
    }

    @Override
    protected RescorerRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (RescorerRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
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
}
