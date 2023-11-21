/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.xpack.application.connector.action.PutConnectorAction;

import java.util.Locale;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;

public final class ConnectorTestUtils {
    public static PutConnectorAction.Request getRandomPutConnectorActionRequest() {
        return new PutConnectorAction.Request(
            randomAlphaOfLengthBetween(5, 15),
            randomAlphaOfLengthBetween(5, 15),
            randomAlphaOfLengthBetween(5, 15),
            randomBoolean(),
            randomAlphaOfLengthBetween(5, 15),
            randomAlphaOfLengthBetween(5, 15),
            randomAlphaOfLengthBetween(5, 15)
        );
    }

    public static ConnectorScheduling getRandomConnectorScheduling() {
        return new ConnectorScheduling.Builder().setAccessControl(
            new ConnectorScheduling.ScheduleConfig.Builder().setEnabled(randomBoolean()).setInterval(getRandomCronExpression()).build()
        )
            .setFull(
                new ConnectorScheduling.ScheduleConfig.Builder().setEnabled(randomBoolean()).setInterval(getRandomCronExpression()).build()
            )
            .setIncremental(
                new ConnectorScheduling.ScheduleConfig.Builder().setEnabled(randomBoolean()).setInterval(getRandomCronExpression()).build()
            )
            .build();
    }

    public static ConnectorIngestPipeline getRandomConnectorIngestPipeline() {
        return new ConnectorIngestPipeline.Builder().setName(randomAlphaOfLengthBetween(5, 15))
            .setExtractBinaryContent(randomBoolean())
            .setReduceWhitespace(randomBoolean())
            .setRunMlInference(randomBoolean())
            .build();
    }

    /**
     * Minute (0 - 59) Hour (0 - 23) Day of month (1 - 28) Month (1 - 12) Day of week (0 - 6)
     */
    private static String getRandomCronExpression() {
        return String.format(
            Locale.ROOT,
            "%d %d %d %d %d",
            randomInt(60),
            randomInt(24),
            randomInt(28) + 1,
            randomInt(12) + 1,
            randomInt(7)
        );
    }
}
