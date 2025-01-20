/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractChunkedBWCSerializationTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RankedDocsResultsTests extends AbstractChunkedBWCSerializationTestCase<RankedDocsResults> {

    @Override
    protected Writeable.Reader<RankedDocsResults> instanceReader() {
        return RankedDocsResults::new;
    }

    @Override
    protected RankedDocsResults createTestInstance() {
        return createRandom();
    }

    public static RankedDocsResults createRandom() {
        return new RankedDocsResults(randomList(0, 10, RankedDocsResultsTests::createRandomDoc));
    }

    public static RankedDocsResults.RankedDoc createRandomDoc() {
        return new RankedDocsResults.RankedDoc(randomIntBetween(0, 100), randomFloat(), randomBoolean() ? null : randomAlphaOfLength(10));
    }

    public void test_asMap() {
        var index = randomIntBetween(0, 100);
        var score = randomFloat();
        var mapNullText = new RankedDocsResults.RankedDoc(index, score, null).asMap();
        assertThat(mapNullText, Matchers.is(Map.of("ranked_doc", Map.of("index", index, "relevance_score", score))));

        var mapWithText = new RankedDocsResults.RankedDoc(index, score, "Sample text").asMap();
        assertThat(mapWithText, Matchers.is(Map.of("ranked_doc", Map.of("index", index, "relevance_score", score, "text", "Sample text"))));
    }

    @Override
    protected RankedDocsResults mutateInstance(RankedDocsResults instance) throws IOException {
        List<RankedDocsResults.RankedDoc> copy = new ArrayList<>(List.copyOf(instance.getRankedDocs()));
        copy.add(createRandomDoc());
        return new RankedDocsResults(copy);
    }

    @Override
    protected RankedDocsResults mutateInstanceForVersion(RankedDocsResults instance, TransportVersion fromVersion) {
        if (fromVersion.onOrAfter(TransportVersions.V_8_15_0)) {
            return instance;
        } else {
            var compatibleDocs = rankedDocsNullStringToEmpty(instance.getRankedDocs());
            return new RankedDocsResults(compatibleDocs);
        }
    }

    private List<RankedDocsResults.RankedDoc> rankedDocsNullStringToEmpty(List<RankedDocsResults.RankedDoc> rankedDocs) {
        var result = new ArrayList<RankedDocsResults.RankedDoc>(rankedDocs.size());
        for (var doc : rankedDocs) {
            if (doc.text() == null) {
                result.add(new RankedDocsResults.RankedDoc(doc.index(), doc.relevanceScore(), ""));
            } else {
                result.add(doc);
            }
        }
        return result;
    }

    @Override
    protected RankedDocsResults doParseInstance(XContentParser parser) throws IOException {
        return RankedDocsResults.createParser(true).apply(parser, null);
    }
}
