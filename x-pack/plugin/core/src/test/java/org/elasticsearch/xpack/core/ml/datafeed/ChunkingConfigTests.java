/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    protected ChunkingConfig doParseInstance(XContentParser parser) {
        return ChunkingConfig.STRICT_PARSER.apply(parser, null);
    }

    public void testConstructorGivenAutoAndTimeSpan() {
        expectThrows(IllegalArgumentException.class, () -> new ChunkingConfig(ChunkingConfig.Mode.AUTO, TimeValue.timeValueMillis(1000)));
    }

    public void testConstructorGivenOffAndTimeSpan() {
        expectThrows(IllegalArgumentException.class, () -> new ChunkingConfig(ChunkingConfig.Mode.OFF, TimeValue.timeValueMillis(1000)));
    }

    public void testConstructorGivenManualAndNoTimeSpan() {
        expectThrows(IllegalArgumentException.class, () -> new ChunkingConfig(ChunkingConfig.Mode.MANUAL, null));
    }

    public void testIsEnabled() {
        assertThat(ChunkingConfig.newAuto().isEnabled(), is(true));
        assertThat(ChunkingConfig.newManual(TimeValue.timeValueMillis(1000)).isEnabled(), is(true));
        assertThat(ChunkingConfig.newOff().isEnabled(), is(false));
    }

    public static ChunkingConfig createRandomizedChunk() {
        ChunkingConfig.Mode mode = randomFrom(ChunkingConfig.Mode.values());
        TimeValue timeSpan = null;
        if (mode == ChunkingConfig.Mode.MANUAL) {
            // time span is required to be at least 1 millis, so we use a custom method to generate a time value here
            timeSpan = randomPositiveSecondsMinutesHours();
        }
        return new ChunkingConfig(mode, timeSpan);
     }

    private static TimeValue randomPositiveSecondsMinutesHours() {
        return new TimeValue(randomIntBetween(1, 1000), randomFrom(Arrays.asList(TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS)));
    }

    @Override
    protected ChunkingConfig mutateInstance(ChunkingConfig instance) throws IOException {
        ChunkingConfig.Mode mode = instance.getMode();
        TimeValue timeSpan = instance.getTimeSpan();
        switch (between(0, 1)) {
        case 0:
            List<ChunkingConfig.Mode> modes = new ArrayList<>(Arrays.asList(ChunkingConfig.Mode.values()));
            modes.remove(mode);
            mode = randomFrom(modes);
            if (mode == ChunkingConfig.Mode.MANUAL) {
                timeSpan = randomPositiveSecondsMinutesHours();
            } else {
                timeSpan = null;
            }
            break;
        case 1:
            if (timeSpan == null) {
                timeSpan = randomPositiveSecondsMinutesHours();
            } else {
                timeSpan = new TimeValue(timeSpan.getMillis() + between(10, 10000));
            }
            // only manual mode allows a timespan
            mode = ChunkingConfig.Mode.MANUAL;
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new ChunkingConfig(mode, timeSpan);
    }
}
