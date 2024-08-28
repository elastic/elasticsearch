/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InputTypeTests.randomWithIngestAndSearch;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<
    AlibabaCloudSearchEmbeddingsTaskSettings> {
    public static AlibabaCloudSearchEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomWithIngestAndSearch() : null;

        return new AlibabaCloudSearchEmbeddingsTaskSettings(inputType);
    }

    public void testFromMap() {
        MatcherAssert.assertThat(
            AlibabaCloudSearchEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(AlibabaCloudSearchEmbeddingsTaskSettings.INPUT_TYPE, "ingest"))
            ),
            is(new AlibabaCloudSearchEmbeddingsTaskSettings(InputType.INGEST))
        );
    }

    public void testFromMap_WhenInputTypeIsNull() {
        InputType inputType = null;
        MatcherAssert.assertThat(
            AlibabaCloudSearchEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(new AlibabaCloudSearchEmbeddingsTaskSettings(inputType))
        );
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchEmbeddingsTaskSettings> instanceReader() {
        return AlibabaCloudSearchEmbeddingsTaskSettings::new;
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsTaskSettings mutateInstance(AlibabaCloudSearchEmbeddingsTaskSettings instance)
        throws IOException {
        return null;
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(AlibabaCloudSearchEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }

        return map;
    }
}
