/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.xpack.application.connector.action.PutConnectorAction;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDateFormatterPattern;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomLong;

public final class ConnectorTestUtils {
    public static PutConnectorAction.Request getRandomPutConnectorActionRequest() {
        return new PutConnectorAction.Request(
            randomAlphaOfLengthBetween(5, 15),
            randomFrom(randomAlphaOfLengthBetween(5, 15)),
            randomFrom(randomAlphaOfLengthBetween(5, 15)),
            randomFrom(randomBoolean()),
            randomFrom(randomAlphaOfLengthBetween(5, 15)),
            randomFrom(randomAlphaOfLengthBetween(5, 15)),
            randomFrom(randomAlphaOfLengthBetween(5, 15))
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

    public static ConnectorSyncInfo getRandomConnectorSyncInfo() {
        return new ConnectorSyncInfo.Builder().setLastAccessControlSyncError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastAccessControlSyncScheduledAt(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastAccessControlSyncStatus(randomFrom(new ConnectorSyncStatus[] { null, getRandomSyncStatus() }))
            .setLastDeletedDocumentCount(randomFrom(new Long[] { null, randomLong() }))
            .setLastIncrementalSyncScheduledAt(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastIndexedDocumentCount(randomFrom(new Long[] { null, randomLong() }))
            .setLastSeen(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastSyncError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastSyncScheduledAt(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastSyncStatus(randomFrom(new ConnectorSyncStatus[] { null, getRandomSyncStatus() }))
            .setLastSynced(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .build();
    }

    public static ConnectorFeatures getRandomConnectorFeatures() {
        return new ConnectorFeatures.Builder().setDocumentLevelSecurityEnabled(randomFrom(new Boolean[] { null, randomBoolean() }))
            .setFilteringRules(randomFrom(new Boolean[] { null, randomBoolean() }))
            .setFilteringAdvancedConfig(randomFrom(new Boolean[] { null, randomBoolean() }))
            .setIncrementalSyncEnabled(randomFrom(new Boolean[] { null, randomBoolean() }))
            .setSyncRulesAdvancedEnabled(randomFrom(new Boolean[] { null, randomBoolean() }))
            .setSyncRulesBasicEnabled(randomFrom(new Boolean[] { null, randomBoolean() }))
            .build();
    }

    public static ConnectorCustomSchedule getRandomConnectorCustomSchedule() {
        return new ConnectorCustomSchedule.Builder().setInterval(getRandomCronExpression())
            .setEnabled(randomBoolean())
            .setLastSynced(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setName(randomAlphaOfLength(10))
            .setConfigurationOverrides(Collections.emptyMap())
            .build();
    }

    public static ConnectorFiltering getRandomConnectorFiltering() {

        String currentTimestamp = randomDateFormatterPattern();

        return new ConnectorFiltering.Builder().setActive(
            new ConnectorFiltering.FilteringRules.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                .setAdvancedSnippetUpdatedAt(currentTimestamp)
                .setAdvancedSnippetValue(Collections.emptyMap())
                .setRules(
                    List.of(
                        new ConnectorFiltering.FilteringRule.Builder().setCreatedAt(currentTimestamp)
                            .setField(randomAlphaOfLength(10))
                            .setId(randomAlphaOfLength(10))
                            .setOrder(randomInt())
                            .setPolicy(getRandomFilteringPolicy())
                            .setRule(getRandomFilteringRule())
                            .setUpdatedAt(currentTimestamp)
                            .setValue(randomAlphaOfLength(10))
                            .build()
                    )
                )
                .setValidationErrors(Collections.emptyList())
                .setValidationState(getRandomFilteringValidationState())
                .build()
        )
            .setDomain(randomAlphaOfLength(10))
            .setDraft(
                new ConnectorFiltering.FilteringRules.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                    .setAdvancedSnippetUpdatedAt(currentTimestamp)
                    .setAdvancedSnippetValue(Collections.emptyMap())
                    .setRules(
                        List.of(
                            new ConnectorFiltering.FilteringRule.Builder().setCreatedAt(currentTimestamp)
                                .setField(randomAlphaOfLength(10))
                                .setId(randomAlphaOfLength(10))
                                .setOrder(randomInt())
                                .setPolicy(getRandomFilteringPolicy())
                                .setRule(getRandomFilteringRule())
                                .setUpdatedAt(currentTimestamp)
                                .setValue(randomAlphaOfLength(10))
                                .build()
                        )
                    )
                    .setValidationErrors(Collections.emptyList())
                    .setValidationState(getRandomFilteringValidationState())
                    .build()
            )
            .build();
    }

    public static Connector getRandomConnector() {
        return new Connector.Builder().setConnectorId(randomAlphaOfLength(10))
            .setApiKeyId(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setConfiguration(randomBoolean() ? Collections.emptyMap() : null)
            .setCustomScheduling(randomBoolean() ? getRandomConnectorCustomSchedule() : null)
            .setDescription(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setFeatures(randomBoolean() ? getRandomConnectorFeatures() : null)
            .setFiltering(randomBoolean() ? List.of(getRandomConnectorFiltering()) : null)
            .setIndexName(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setIsNative(randomFrom(new Boolean[] { null, randomBoolean() }))
            .setLanguage(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setSyncInfo(randomBoolean() ? getRandomConnectorSyncInfo() : null)
            .setName(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setPipeline(randomBoolean() ? getRandomConnectorIngestPipeline() : null)
            .setScheduling(randomBoolean() ? getRandomConnectorScheduling() : null)
            .setStatus(getRandomConnectorStatus())
            .setSyncCursor(randomFrom(new Object[] { null, randomAlphaOfLength(1) }))
            .setSyncNow(randomFrom(new Boolean[] { null, randomBoolean() }))
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

    private static ConnectorSyncStatus getRandomSyncStatus() {
        ConnectorSyncStatus[] values = ConnectorSyncStatus.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConnectorStatus getRandomConnectorStatus() {
        ConnectorStatus[] values = ConnectorStatus.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConnectorFiltering.FilteringPolicy getRandomFilteringPolicy() {
        ConnectorFiltering.FilteringPolicy[] values = ConnectorFiltering.FilteringPolicy.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConnectorFiltering.FilteringRuleRule getRandomFilteringRule() {
        ConnectorFiltering.FilteringRuleRule[] values = ConnectorFiltering.FilteringRuleRule.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConnectorFiltering.FilteringValidationState getRandomFilteringValidationState() {
        ConnectorFiltering.FilteringValidationState[] values = ConnectorFiltering.FilteringValidationState.values();
        return values[randomInt(values.length - 1)];
    }
}
