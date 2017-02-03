/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.is;

public class ChunkingConfigTests extends AbstractSerializingTestCase<ChunkingConfig> {

    @Override
    protected ChunkingConfig createTestInstance() {
        return createRandomizedChunk();
    }

    @Override
    protected Writeable.Reader<ChunkingConfig> instanceReader() {
        return ChunkingConfig::new;
    }

    @Override
    protected ChunkingConfig parseInstance(XContentParser parser) {
        return ChunkingConfig.PARSER.apply(parser, null);
    }

    public void testConstructorGivenAutoAndTimeSpan() {
        expectThrows(IllegalArgumentException.class, () ->new ChunkingConfig(ChunkingConfig.Mode.AUTO, 1000L));
    }

    public void testConstructorGivenOffAndTimeSpan() {
        expectThrows(IllegalArgumentException.class, () ->new ChunkingConfig(ChunkingConfig.Mode.OFF, 1000L));
    }

    public void testConstructorGivenManualAndNoTimeSpan() {
        expectThrows(IllegalArgumentException.class, () ->new ChunkingConfig(ChunkingConfig.Mode.MANUAL, null));
    }

    public void testIsEnabled() {
        assertThat(ChunkingConfig.newAuto().isEnabled(), is(true));
        assertThat(ChunkingConfig.newManual(1000).isEnabled(), is(true));
        assertThat(ChunkingConfig.newOff().isEnabled(), is(false));
    }

    public static ChunkingConfig createRandomizedChunk() {
        ChunkingConfig.Mode mode = randomFrom(ChunkingConfig.Mode.values());
        Long timeSpan = null;
        if (mode == ChunkingConfig.Mode.MANUAL) {
            timeSpan = randomNonNegativeLong();
            if (timeSpan == 0L) {
                timeSpan = 1L;
            }
        }
        return new ChunkingConfig(mode, timeSpan);
     }
}