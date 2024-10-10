/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InputTypeTests.randomWithIngestAndSearch;
import static org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseTaskSettings.INPUT_TYPE;
import static org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseTaskSettings.RETURN_TOKEN;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchSparseTaskSettingsTests extends AbstractWireSerializingTestCase<AlibabaCloudSearchSparseTaskSettings> {
    public static AlibabaCloudSearchSparseTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomWithIngestAndSearch() : null;
        var returnToken = randomBoolean();

        return new AlibabaCloudSearchSparseTaskSettings(inputType, returnToken);
    }

    public void testFromMap() {
        MatcherAssert.assertThat(
            AlibabaCloudSearchSparseTaskSettings.fromMap(new HashMap<>(Map.of(INPUT_TYPE, "ingest"))),
            is(new AlibabaCloudSearchSparseTaskSettings(InputType.INGEST, null))
        );
    }

    public void testUpdatedTaskSettings() {
        {
            var initialSettings = createRandom();
            var newSettings = createRandom();
            AlibabaCloudSearchSparseTaskSettings updatedSettings = (AlibabaCloudSearchSparseTaskSettings) initialSettings
                .updatedTaskSettings(Map.of(RETURN_TOKEN, newSettings.isReturnToken()));
        }
        {
            var initialSettings = createRandom();
            var newSettings = createRandom();
            AlibabaCloudSearchSparseTaskSettings updatedSettings = (AlibabaCloudSearchSparseTaskSettings) initialSettings
                .updatedTaskSettings(
                    Map.of(
                        INPUT_TYPE,
                        newSettings.getInputType() == null ? InputType.SEARCH.toString() : newSettings.getInputType().toString(),
                        RETURN_TOKEN,
                        newSettings.isReturnToken()
                    )
                );
        }
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testFromMap_WhenInputTypeIsNull() {
        InputType inputType = null;
        MatcherAssert.assertThat(
            AlibabaCloudSearchSparseTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(new AlibabaCloudSearchSparseTaskSettings(inputType, null))
        );
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchSparseTaskSettings> instanceReader() {
        return AlibabaCloudSearchSparseTaskSettings::new;
    }

    @Override
    protected AlibabaCloudSearchSparseTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchSparseTaskSettings mutateInstance(AlibabaCloudSearchSparseTaskSettings instance) throws IOException {
        return null;
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType, @Nullable Boolean returnToken) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(INPUT_TYPE, inputType.toString());
        }

        if (returnToken != null) {
            map.put(RETURN_TOKEN, returnToken);
        }

        return map;
    }
}
