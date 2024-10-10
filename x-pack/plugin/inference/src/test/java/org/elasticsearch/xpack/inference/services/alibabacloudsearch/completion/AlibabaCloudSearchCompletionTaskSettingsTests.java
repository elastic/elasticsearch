/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchCompletionTaskSettingsTests extends AbstractWireSerializingTestCase<
    AlibabaCloudSearchCompletionTaskSettings> {
    public static AlibabaCloudSearchCompletionTaskSettings createRandom() {
        Map<String, Object> parameters = randomBoolean() ? Map.of() : null;

        return new AlibabaCloudSearchCompletionTaskSettings(parameters);
    }

    public void testFromMap() {
        MatcherAssert.assertThat(
            AlibabaCloudSearchCompletionTaskSettings.fromMap(Map.of()),
            is(new AlibabaCloudSearchCompletionTaskSettings((Map<String, Object>) null))
        );
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchCompletionTaskSettings> instanceReader() {
        return AlibabaCloudSearchCompletionTaskSettings::new;
    }

    @Override
    protected AlibabaCloudSearchCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchCompletionTaskSettings mutateInstance(AlibabaCloudSearchCompletionTaskSettings instance)
        throws IOException {
        return null;
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Map<String, Object> params) {
        var map = new HashMap<String, Object>();

        if (params != null) {
            map.put(AlibabaCloudSearchCompletionTaskSettings.PARAMETERS, params);
        }

        return map;
    }
}
