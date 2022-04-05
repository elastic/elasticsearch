/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

abstract class AbstractNlpConfigUpdateTestCase<T extends NlpConfigUpdate> extends AbstractBWCSerializationTestCase<T> {

    /**
     * @param expectedTokenization The tokenization update that will be provided
     * @return A map and expected resulting object. Note: `tokenization` will be overwritten if provided in the returned map
     */
    abstract Tuple<Map<String, Object>, T> fromMapTestInstances(TokenizationUpdate expectedTokenization);

    /**
     * @param map The map of options
     * @return A NlpConfigUpdate object
     */
    abstract T fromMap(Map<String, Object> map);

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    public void testFromMap() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            final String tokenizationKind;
            final TokenizationUpdate update;
            final Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
            int testCase = randomInt(2);
            switch (testCase) {
                case 0 -> {
                    tokenizationKind = "bert";
                    update = new BertTokenizationUpdate(truncate, null);
                }
                case 1 -> {
                    tokenizationKind = "mpnet";
                    update = new MPNetTokenizationUpdate(truncate, null);
                }
                case 2 -> {
                    tokenizationKind = "roberta";
                    update = new RobertaTokenizationUpdate(truncate, null);
                }
                default -> throw new UnsupportedOperationException("unexpected test case");

            }
            var expected = fromMapTestInstances(update);
            Map<String, Object> config = new HashMap<>(expected.v1());
            Map<String, Object> tokenizationConfig = new HashMap<>() {
                {
                    put(tokenizationKind, new HashMap<>() {
                        {
                            put("truncate", truncate.toString());
                        }
                    });
                }
            };
            config.put("tokenization", tokenizationConfig);
            assertFromMapEquality(expected.v2(), fromMap(config));
        }
    }

    void assertFromMapEquality(T expected, T parsedFromMap) {
        assertThat(parsedFromMap, equalTo(expected));
    }
}
