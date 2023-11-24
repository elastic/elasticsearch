/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.xpack.application.connector.action.PutConnectorAction;
import org.elasticsearch.xpack.core.scheduler.Cron;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomList;
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
            .setLastAccessControlSyncScheduledAt(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .setLastAccessControlSyncStatus(randomFrom(new ConnectorSyncStatus[] { null, getRandomSyncStatus() }))
            .setLastDeletedDocumentCount(randomFrom(new Long[] { null, randomLong() }))
            .setLastIncrementalSyncScheduledAt(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .setLastIndexedDocumentCount(randomFrom(new Long[] { null, randomLong() }))
            .setLastSeen(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .setLastSyncError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastSyncScheduledAt(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .setLastSyncStatus(randomFrom(new ConnectorSyncStatus[] { null, getRandomSyncStatus() }))
            .setLastSynced(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
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
            .setLastSynced(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .setName(randomAlphaOfLength(10))
            .setConfigurationOverrides(
                new ConnectorCustomSchedule.ConfigurationOverrides.Builder().setMaxCrawlDepth(randomInt())
                    .setSitemapDiscoveryDisabled(randomBoolean())
                    .setDomainAllowList(randomList(1, 5, () -> randomAlphaOfLength(5)))
                    .setSeedUrls(randomList(1, 5, () -> randomAlphaOfLength(5)))
                    .setSitemapUrls(randomList(1, 5, () -> randomAlphaOfLength(5)))
                    .build()
            )
            .build();
    }

    public static ConnectorFiltering getRandomConnectorFiltering() {

        Instant currentTimestamp = Instant.now();

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
            .setConfiguration(Collections.emptyMap())
            .setCustomScheduling(randomBoolean() ? getRandomConnectorCustomSchedule() : null)
            .setDescription(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setFeatures(randomBoolean() ? getRandomConnectorFeatures() : null)
            .setFiltering(randomBoolean() ? List.of(getRandomConnectorFiltering()) : null)
            .setIndexName(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setIsNative(randomBoolean())
            .setLanguage(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setSyncInfo(getRandomConnectorSyncInfo())
            .setName(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setPipeline(randomBoolean() ? getRandomConnectorIngestPipeline() : null)
            .setScheduling(randomBoolean() ? getRandomConnectorScheduling() : null)
            .setStatus(getRandomConnectorStatus())
            .setSyncCursor(randomFrom(new Object[] { null, randomAlphaOfLength(1) }))
            .setSyncNow(randomBoolean())
            .build();
    }

    /**
     * Second (0 - 59) Minute (0 - 59) Hour (0 - 23) Day of month (1 - 31) Month (1 - 12)
     */
    private static Cron getRandomCronExpression() {
        return new Cron(
            String.format(
                Locale.ROOT,
                "%d %d %d %d %d ?",
                randomInt(59),
                randomInt(59),
                randomInt(23),
                randomInt(30) + 1,
                randomInt(11) + 1
            )
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

    private static ConnectorFiltering.FilteringRuleCondition getRandomFilteringRule() {
        ConnectorFiltering.FilteringRuleCondition[] values = ConnectorFiltering.FilteringRuleCondition.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConnectorFiltering.FilteringValidationState getRandomFilteringValidationState() {
        ConnectorFiltering.FilteringValidationState[] values = ConnectorFiltering.FilteringValidationState.values();
        return values[randomInt(values.length - 1)];
    }
}
