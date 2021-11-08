/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.elasticsearch.xpack.ml.action.TransportPutTrainedModelDefinitionPartAction.getBase64DecodedByteLength;
import static org.hamcrest.Matchers.equalTo;

public class TransportPutTrainedModelDefinitionPartActionTests extends ESTestCase {

    public void testGetBase64DecodedByteLength() {
        for (int i = 0; i < 10; i++) {
            String str = randomAlphaOfLengthBetween(1, 10_000_000);
            int expected = str.getBytes(StandardCharsets.UTF_8).length;
            assertThat(getBase64DecodedByteLength(Base64.getEncoder().encode(str.getBytes(StandardCharsets.UTF_8))), equalTo(expected));
        }
        String str = randomAlphaOfLength(1);
        int expected = str.getBytes(StandardCharsets.UTF_8).length;
        assertThat(getBase64DecodedByteLength(Base64.getEncoder().encode(str.getBytes(StandardCharsets.UTF_8))), equalTo(expected));

        str = "";
        assertThat(getBase64DecodedByteLength(Base64.getEncoder().encode(str.getBytes(StandardCharsets.UTF_8))), equalTo(0));

    }

}
