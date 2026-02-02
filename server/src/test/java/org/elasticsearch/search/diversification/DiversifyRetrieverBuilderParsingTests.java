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
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class DiversifyRetrieverBuilderParsingTests extends AbstractXContentTestCase<DiversifyRetrieverBuilder> {
    private static final TransportVersion QUERY_VECTOR_BASE64 = TransportVersion.fromName("knn_query_vector_base64");
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
    protected DiversifyRetrieverBuilder createTestInstance() {
        int rankWindowSize = randomIntBetween(1, 20);
        Integer size = randomBoolean() ? null : randomIntBetween(1, 20);
        VectorData queryVector = randomBoolean() ? getRandomQueryVector() : null;
        Float lambda = randomBoolean() ? randomFloatBetween(0.0f, 1.0f, true) : null;
        CompoundRetrieverBuilder.RetrieverSource innerRetriever = new CompoundRetrieverBuilder.RetrieverSource(
            TestRetrieverBuilder.createRandomTestRetrieverBuilder(),
            null
        );
        return new DiversifyRetrieverBuilder(
            innerRetriever,
            ResultDiversificationType.MMR,
            "test_field",
            rankWindowSize,
            size,
            queryVector,
            null,
            lambda
        );
    }

    @Override
    protected DiversifyRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (DiversifyRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
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

    @Override
    protected void assertEqualInstances(DiversifyRetrieverBuilder expectedInstance, DiversifyRetrieverBuilder newInstance) {
        if (TransportVersion.current().supports(QUERY_VECTOR_BASE64) == false) {
            super.assertEqualInstances(expectedInstance, newInstance);
            return;
        }

        try {
            super.assertEqualInstances(expectedInstance, newInstance);
        } catch (AssertionError e) {
            try {
                assertToXContentEquivalent(
                    XContentHelper.toXContent(expectedInstance, XContentType.JSON, false),
                    XContentHelper.toXContent(newInstance, XContentType.JSON, false),
                    XContentType.JSON
                );
            } catch (IOException ioException) {
                throw new AssertionError(ioException);
            }
        }
    }

    private VectorData getRandomQueryVector() {
        if (randomBoolean()) {
            return new VectorData(getRandomFloatQueryVector());
        }

        byte[] queryVector = new byte[randomIntBetween(5, 256)];
        for (int i = 0; i < queryVector.length; i++) {
            queryVector[i] = randomByte();
        }
        return new VectorData(queryVector);
    }

    private float[] getRandomFloatQueryVector() {
        float[] queryVector = new float[randomIntBetween(5, 256)];
        for (int i = 0; i < queryVector.length; i++) {
            queryVector[i] = randomFloatBetween(0.0f, 1.0f, true);
        }
        return queryVector;
    }
}
