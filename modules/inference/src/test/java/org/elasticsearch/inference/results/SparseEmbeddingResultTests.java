/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class SparseEmbeddingResultTests extends AbstractXContentSerializingTestCase<SparseEmbeddingResult> {

    public static SparseEmbeddingResult createRandomResult() {
        int numTokens = randomIntBetween(1, 20);
        List<SparseEmbeddingResult.WeightedToken> tokenList = new ArrayList<>();
        for (int i = 0; i < numTokens; i++) {
            tokenList.add(new SparseEmbeddingResult.WeightedToken(Integer.toString(i), (float) randomDoubleBetween(0.0, 5.0, false)));
        }
        return new SparseEmbeddingResult(tokenList);
    }

    @Override
    protected Writeable.Reader<SparseEmbeddingResult> instanceReader() {
        return SparseEmbeddingResult::new;
    }

    @Override
    protected SparseEmbeddingResult createTestInstance() {
        return createRandomResult();
    }

    @Override
    protected SparseEmbeddingResult mutateInstance(SparseEmbeddingResult instance) {
        if (instance.getWeightedTokens().size() > 0) {
            var tokens = instance.getWeightedTokens();
            return new SparseEmbeddingResult(tokens.subList(0, tokens.size() - 1));
        } else {
            return new SparseEmbeddingResult(List.of(new SparseEmbeddingResult.WeightedToken("a", 1.0f)));
        }
    }

    @Override
    protected SparseEmbeddingResult doParseInstance(XContentParser parser) {
        var resultParser = createParser();
        return resultParser.apply(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<SparseEmbeddingResult, Void> createParser() {
        ConstructingObjectParser<SparseEmbeddingResult, Void> parser = new ConstructingObjectParser<>(
            "test",
            false,
            a -> new SparseEmbeddingResult((List<SparseEmbeddingResult.WeightedToken>) a[0])
        );
        parser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {

            var weightedTokens = new ArrayList<SparseEmbeddingResult.WeightedToken>();
            XContentParser.Token token = p.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, p);
            token = p.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, p);
                String fieldName = p.currentName();
                token = p.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, p);
                float value = p.floatValue();
                weightedTokens.add(new SparseEmbeddingResult.WeightedToken(fieldName, value));
                token = p.nextToken();
            }
            return weightedTokens;
        }, new ParseField("sparse_embedding"));

        return parser;
    }
}
