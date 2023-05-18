/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm.dataperiods;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class DataPeriodTests extends AbstractXContentTestCase<DataPeriod> {

    @Override
    protected DataPeriod createTestInstance() {
        return randomDataPeriod();
    }

    static DataPeriod randomDataPeriod() {
        return new DataPeriod(
            List.of(randomAlphaOfLength(10) + "*"),
            randomBoolean() ? TimeValue.timeValueDays(randomIntBetween(1, 10)) : null,
            randomBoolean() ? TimeValue.timeValueDays(randomIntBetween(30, 365)) : null,
            randomNonNegativeInt()
        );
    }

    @Override
    protected DataPeriod doParseInstance(XContentParser parser) throws IOException {
        return DataPeriod.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testInvalidDataPeriods() {
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> new DataPeriod(List.of("*-logs"), null, null, 0)
            );
            assertThat(
                error.getMessage(),
                containsString(
                    "Name pattern '*-logs' does not match the allowed pattern styles: \"xxx*\", \"*\" or a concrete data stream name"
                )
            );
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> new DataPeriod(List.of("logs-*-suffix"), null, null, 0)
            );
            assertThat(
                error.getMessage(),
                containsString(
                    "Name pattern 'logs-*-suffix' does not match the allowed pattern styles: \"xxx*\", \"*\" or a concrete data stream name"
                )
            );
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> new DataPeriod(List.of("logs-*"), TimeValue.timeValueDays(10), TimeValue.timeValueDays(1), 0)
            );
            assertThat(error.getMessage(), containsString("The interactivity period of your data "));
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> new DataPeriod(List.of("logs-*"), TimeValue.timeValueDays(10), null, -1)
            );
            assertThat(error.getMessage(), containsString("Data period cannot have negative priority."));
        }
    }
}
