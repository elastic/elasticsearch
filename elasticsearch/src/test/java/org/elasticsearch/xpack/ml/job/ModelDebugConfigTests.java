/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.ModelDebugConfig.DebugDestination;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

public class ModelDebugConfigTests extends AbstractSerializingTestCase<ModelDebugConfig> {

    public void testEquals() {
        assertFalse(new ModelDebugConfig(0d, null).equals(null));
        assertFalse(new ModelDebugConfig(0d, null).equals("a string"));
        assertFalse(new ModelDebugConfig(80.0, "").equals(new ModelDebugConfig(81.0, "")));
        assertFalse(new ModelDebugConfig(80.0, "foo").equals(new ModelDebugConfig(80.0, "bar")));
        assertFalse(new ModelDebugConfig(DebugDestination.FILE, 80.0, "foo")
                .equals(new ModelDebugConfig(DebugDestination.DATA_STORE, 80.0, "foo")));

        ModelDebugConfig modelDebugConfig = new ModelDebugConfig(0d, null);
        assertTrue(modelDebugConfig.equals(modelDebugConfig));
        assertTrue(new ModelDebugConfig(0d, null).equals(new ModelDebugConfig(0d, null)));
        assertTrue(new ModelDebugConfig(80.0, "foo").equals(new ModelDebugConfig(80.0, "foo")));
        assertTrue(new ModelDebugConfig(DebugDestination.FILE, 80.0, "foo").equals(new ModelDebugConfig(80.0, "foo")));
        assertTrue(new ModelDebugConfig(DebugDestination.DATA_STORE, 80.0, "foo")
                .equals(new ModelDebugConfig(DebugDestination.DATA_STORE, 80.0, "foo")));
    }

    public void testHashCode() {
        assertEquals(new ModelDebugConfig(80.0, "foo").hashCode(), new ModelDebugConfig(80.0, "foo").hashCode());
        assertEquals(new ModelDebugConfig(DebugDestination.FILE, 80.0, "foo").hashCode(), new ModelDebugConfig(80.0, "foo").hashCode());
        assertEquals(new ModelDebugConfig(DebugDestination.DATA_STORE, 80.0, "foo").hashCode(),
                new ModelDebugConfig(DebugDestination.DATA_STORE, 80.0, "foo").hashCode());
    }

    public void testVerify_GivenBoundPercentileLessThanZero() {
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> new ModelDebugConfig(-1.0, ""));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_MODEL_DEBUG_CONFIG_INVALID_BOUNDS_PERCENTILE, ""), e.getMessage());
    }

    public void testVerify_GivenBoundPercentileGreaterThan100() {
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> new ModelDebugConfig(100.1, ""));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_MODEL_DEBUG_CONFIG_INVALID_BOUNDS_PERCENTILE, ""), e.getMessage());
    }

    public void testVerify_GivenValid() {
        new ModelDebugConfig(93.0, "");
        new ModelDebugConfig(93.0, "foo,bar");
    }

    @Override
    protected ModelDebugConfig createTestInstance() {
        return new ModelDebugConfig(randomFrom(DebugDestination.values()), randomDouble(), randomAsciiOfLengthBetween(1, 30));
    }

    @Override
    protected Reader<ModelDebugConfig> instanceReader() {
        return ModelDebugConfig::new;
    }

    @Override
    protected ModelDebugConfig parseInstance(XContentParser parser) {
        return ModelDebugConfig.PARSER.apply(parser, null);
    }
}
