/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.TextEmbedding;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class MultilingualE5SmallServiceSettingsTests extends ESTestCase {

    @Test
    public void fromMap() {}

    public static MultilingualE5SmallServiceSettings createRandom() {
        return new MultilingualE5SmallServiceSettings(
            randomIntBetween(1, 4),
            randomIntBetween(1, 4),
            randomFrom(MultilingualE5SmallServiceSettings.MODEL_VARIANTS)
        );
    }
}
