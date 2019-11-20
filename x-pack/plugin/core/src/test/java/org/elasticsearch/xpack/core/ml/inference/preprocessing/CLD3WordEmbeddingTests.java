/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ContinuousFeatureValue;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.FeatureValue;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ScriptDetector;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.hamcrest.Matchers.instanceOf;


public class CLD3WordEmbeddingTests extends PreProcessingTests<CLD3WordEmbedding> {

    @Override
    protected CLD3WordEmbedding doParseInstance(XContentParser parser) throws IOException {
        return lenient ? CLD3WordEmbedding.fromXContentLenient(parser) : CLD3WordEmbedding.fromXContentStrict(parser);
    }

    @Override
    protected CLD3WordEmbedding createTestInstance() {
        return createRandom();
    }

    public static CLD3WordEmbedding createRandom() {
        int quantileSize = randomIntBetween(1, 10);
        int internalQuantSize = randomIntBetween(1, 10);
        short[][] quantiles = new short[quantileSize][internalQuantSize];
        for (int i = 0; i < quantileSize; i++) {
            for (int j = 0; j < internalQuantSize; j++) {
                quantiles[i][j] = randomShort();
            }
        }
        int weightsSize = randomIntBetween(1, 10);
        int internalWeightsSize = randomIntBetween(1, 10);
        int[][] weights = new int[weightsSize][internalWeightsSize];
        for (int i = 0; i < weightsSize; i++) {
            for (int j = 0; j < internalWeightsSize; j++) {
                weights[i][j] = randomInt((int)Character.MAX_VALUE);
            }
        }
        return new CLD3WordEmbedding(quantiles, weights, randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<CLD3WordEmbedding> instanceReader() {
        return CLD3WordEmbedding::new;
    }

    private void testEvaluateRelevantScriptResults(FeatureValue[] results,
                                     int size,
                                     int index,
                                     ScriptDetector.Script id,
                                     float weight) {
        assertEquals(size, results.length);
        assertTrue(index >= 0 && index < results.length);
        assertThat(results[index], instanceOf(ContinuousFeatureValue.class));
        assertEquals(results[index].getRow(), id.toInt());
        assertEquals(results[index].getWeight(), weight, 0.0001f);
    }

    public void testRelevantScriptFeatureCommonCases() throws UnsupportedEncodingException {
        FeatureValue[] results;

        results = CLD3WordEmbedding.getRelevantScriptFeature("just some plain text");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptOtherUtf8OneByte, 1.0f );

        results = CLD3WordEmbedding.getRelevantScriptFeature("ヸヂ゠ヂ");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptKatakana, 1.0f );

        // 4 Latin letters mixed with 4 Katakana letters.
        results = CLD3WordEmbedding.getRelevantScriptFeature("ヸtヂe゠xtヂ");
        testEvaluateRelevantScriptResults(results, 2, 0, ScriptDetector.Script.kScriptOtherUtf8OneByte, 0.5f );
        testEvaluateRelevantScriptResults(results, 2, 1, ScriptDetector.Script.kScriptKatakana, 0.5f );

        results = CLD3WordEmbedding.getRelevantScriptFeature("\"just some 121212%^^( ヸヂ゠ヂ   text\"");
        testEvaluateRelevantScriptResults(results, 2, 0, ScriptDetector.Script.kScriptOtherUtf8OneByte, 0.75f );
        testEvaluateRelevantScriptResults(results, 2, 1, ScriptDetector.Script.kScriptKatakana, 0.25f );
    }

    public void testRelevantScriptFeatureCornerCases() throws UnsupportedEncodingException {
        FeatureValue[] results;

        // Empty string.
        results = CLD3WordEmbedding.getRelevantScriptFeature("");
        assertEquals(0, results.length);

        // Only whitespaces.
        results = CLD3WordEmbedding.getRelevantScriptFeature("   ");
        assertEquals(0, results.length);

        // Only numbers and punctuation.
        results = CLD3WordEmbedding.getRelevantScriptFeature("12----)(");
        assertEquals(0, results.length);

        // Only numbers, punctuation, and spaces.
        results = CLD3WordEmbedding.getRelevantScriptFeature("12--- - ) ( ");
        assertEquals(0, results.length);

        // One UTF8 character by itself.
        results = CLD3WordEmbedding.getRelevantScriptFeature("ゟ");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHiragana, 1.0f );

        results = CLD3WordEmbedding.getRelevantScriptFeature("ה");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHebrew, 1.0f );

        // One UTF8 character with some numbers / punctuation / spaces: character at
        // one extremity or in the middle.
        results = CLD3WordEmbedding.getRelevantScriptFeature("1234ゟ");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHiragana, 1.0f );

        results = CLD3WordEmbedding.getRelevantScriptFeature("ゟ12-(");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHiragana, 1.0f );

        results = CLD3WordEmbedding.getRelevantScriptFeature("8*1ゟ12----");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHiragana, 1.0f );
    }

}
