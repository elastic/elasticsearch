/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;

import static org.hamcrest.Matchers.containsString;

public class MultilingualE5SmallModelTests extends ESTestCase {

    public void testHugeChunkingSettings() {
        Exception expectedException = expectThrows(
            IllegalArgumentException.class,
            () -> new MultilingualE5SmallModel(
                "foo",
                TaskType.TEXT_EMBEDDING,
                ElasticsearchInternalService.NAME,
                MultilingualE5SmallInternalServiceSettings.defaultEndpointSettings(randomBoolean()),
                new SentenceBoundaryChunkingSettings(10000, 0)
            )
        );

        assertThat(
            expectedException.getMessage(),
            containsString("does not support chunk sizes larger than 300. Requested chunk size: 10000")
        );
    }
}
