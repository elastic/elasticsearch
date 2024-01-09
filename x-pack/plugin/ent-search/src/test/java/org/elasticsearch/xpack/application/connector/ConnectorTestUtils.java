/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.xpack.application.connector.action.PostConnectorAction;
import org.elasticsearch.xpack.application.connector.action.PutConnectorAction;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationDependency;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationDisplayType;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationFieldType;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationSelectOption;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationValidation;
import org.elasticsearch.xpack.application.connector.configuration.ConfigurationValidationType;
import org.elasticsearch.xpack.application.connector.filtering.FilteringAdvancedSnippet;
import org.elasticsearch.xpack.application.connector.filtering.FilteringPolicy;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRule;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRuleCondition;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRules;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationInfo;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationState;
import org.elasticsearch.xpack.core.scheduler.Cron;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;

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

    public static PostConnectorAction.Request getRandomPostConnectorActionRequest() {
        return new PostConnectorAction.Request(
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
            .setLastDeletedDocumentCount(randomLong())
            .setLastIncrementalSyncScheduledAt(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .setLastIndexedDocumentCount(randomLong())
            .setLastSyncError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastSyncScheduledAt(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .setLastSyncStatus(randomFrom(new ConnectorSyncStatus[] { null, getRandomSyncStatus() }))
            .setLastSynced(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .build();
    }

    public static ConnectorFeatures getRandomConnectorFeatures() {
        return new ConnectorFeatures.Builder().setDocumentLevelSecurityEnabled(randomBoolean() ? randomConnectorFeatureEnabled() : null)
            .setFilteringRules(randomFrom(new Boolean[] { null, randomBoolean() }))
            .setFilteringAdvancedConfig(randomFrom(new Boolean[] { null, randomBoolean() }))
            .setIncrementalSyncEnabled(randomBoolean() ? randomConnectorFeatureEnabled() : null)
            .setSyncRulesFeatures(randomBoolean() ? randomSyncRulesFeatures() : null)
            .build();
    }

    public static ConnectorCustomSchedule getRandomConnectorCustomSchedule() {
        return new ConnectorCustomSchedule.Builder().setInterval(getRandomCronExpression())
            .setEnabled(randomBoolean())
            .setLastSynced(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLongBetween(0, 10000)) }))
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
            new FilteringRules.Builder().setAdvancedSnippet(
                new FilteringAdvancedSnippet.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                    .setAdvancedSnippetUpdatedAt(currentTimestamp)
                    .setAdvancedSnippetValue(Collections.emptyMap())
                    .build()
            )
                .setRules(
                    List.of(
                        new FilteringRule.Builder().setCreatedAt(currentTimestamp)
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
                .setFilteringValidationInfo(
                    new FilteringValidationInfo.Builder().setValidationErrors(Collections.emptyList())
                        .setValidationState(getRandomFilteringValidationState())
                        .build()
                )
                .build()
        )
            .setDomain(randomAlphaOfLength(10))
            .setDraft(
                new FilteringRules.Builder().setAdvancedSnippet(
                    new FilteringAdvancedSnippet.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                        .setAdvancedSnippetUpdatedAt(currentTimestamp)
                        .setAdvancedSnippetValue(Collections.emptyMap())
                        .build()
                )
                    .setRules(
                        List.of(
                            new FilteringRule.Builder().setCreatedAt(currentTimestamp)
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
                    .setFilteringValidationInfo(
                        new FilteringValidationInfo.Builder().setValidationErrors(Collections.emptyList())
                            .setValidationState(getRandomFilteringValidationState())
                            .build()
                    )
                    .build()
            )
            .build();
    }

    public static Connector getRandomSyncJobConnectorInfo() {
        return new Connector.Builder().setConnectorId(randomAlphaOfLength(10))
            .setFiltering(List.of(getRandomConnectorFiltering()))
            .setIndexName(randomAlphaOfLength(10))
            .setLanguage(randomAlphaOfLength(10))
            .setServiceType(randomAlphaOfLength(10))
            .setConfiguration(Collections.emptyMap())
            .build();
    }

    private static ConfigurationDependency getRandomConfigurationDependency() {
        return new ConfigurationDependency.Builder().setField(randomAlphaOfLength(10)).setValue(randomAlphaOfLength(10)).build();
    }

    private static ConfigurationSelectOption getRandomConfigurationSelectOption() {
        return new ConfigurationSelectOption.Builder().setLabel(randomAlphaOfLength(10)).setValue(randomAlphaOfLength(10)).build();
    }

    private static ConfigurationValidation getRandomConfigurationValidation() {
        return new ConfigurationValidation.Builder().setConstraint(randomAlphaOfLength(10))
            .setType(getRandomConfigurationValidationType())
            .build();
    }

    public static ConnectorConfiguration getRandomConnectorConfigurationField() {
        return new ConnectorConfiguration.Builder().setCategory(randomAlphaOfLength(10))
            .setDefaultValue(randomAlphaOfLength(10))
            .setDependsOn(List.of(getRandomConfigurationDependency()))
            .setDisplay(getRandomConfigurationDisplayType())
            .setLabel(randomAlphaOfLength(10))
            .setOptions(List.of(getRandomConfigurationSelectOption(), getRandomConfigurationSelectOption()))
            .setOrder(randomInt())
            .setPlaceholder(randomAlphaOfLength(10))
            .setRequired(randomBoolean())
            .setSensitive(randomBoolean())
            .setTooltip(randomAlphaOfLength(10))
            .setType(getRandomConfigurationFieldType())
            .setUiRestrictions(List.of(randomAlphaOfLength(10), randomAlphaOfLength(10)))
            .setValidations(List.of(getRandomConfigurationValidation()))
            .setValue(randomAlphaOfLength(10))
            .build();
    }

    public static Map<String, ConnectorConfiguration> getRandomConnectorConfiguration() {
        Map<String, ConnectorConfiguration> configMap = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            configMap.put(randomAlphaOfLength(10), getRandomConnectorConfigurationField());
        }
        return configMap;
    }

    public static Connector getRandomConnector() {

        return new Connector.Builder().setApiKeyId(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setConfiguration(getRandomConnectorConfiguration())
            .setCustomScheduling(Map.of(randomAlphaOfLengthBetween(5, 10), getRandomConnectorCustomSchedule()))
            .setDescription(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setFeatures(randomBoolean() ? getRandomConnectorFeatures() : null)
            .setFiltering(List.of(getRandomConnectorFiltering()))
            .setIndexName(randomAlphaOfLength(10))
            .setIsNative(randomBoolean())
            .setLanguage(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastSeen(randomFrom(new Instant[] { null, Instant.ofEpochMilli(randomLong()) }))
            .setSyncInfo(getRandomConnectorSyncInfo())
            .setName(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setPipeline(randomBoolean() ? getRandomConnectorIngestPipeline() : null)
            .setScheduling(getRandomConnectorScheduling())
            .setStatus(getRandomConnectorStatus())
            .setSyncCursor(randomBoolean() ? Map.of(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)) : null)
            .setSyncNow(randomBoolean())
            .build();
    }

    private static ConnectorFeatures.FeatureEnabled randomConnectorFeatureEnabled() {
        return new ConnectorFeatures.FeatureEnabled(randomBoolean());
    }

    private static ConnectorFeatures.SyncRulesFeatures randomSyncRulesFeatures() {
        return new ConnectorFeatures.SyncRulesFeatures.Builder().setSyncRulesAdvancedEnabled(
            randomBoolean() ? randomConnectorFeatureEnabled() : null
        ).setSyncRulesBasicEnabled(randomBoolean() ? randomConnectorFeatureEnabled() : null).build();
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

    public static ConnectorSyncStatus getRandomSyncStatus() {
        ConnectorSyncStatus[] values = ConnectorSyncStatus.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConnectorStatus getRandomConnectorStatus() {
        ConnectorStatus[] values = ConnectorStatus.values();
        return values[randomInt(values.length - 1)];
    }

    private static FilteringPolicy getRandomFilteringPolicy() {
        FilteringPolicy[] values = FilteringPolicy.values();
        return values[randomInt(values.length - 1)];
    }

    private static FilteringRuleCondition getRandomFilteringRule() {
        FilteringRuleCondition[] values = FilteringRuleCondition.values();
        return values[randomInt(values.length - 1)];
    }

    private static FilteringValidationState getRandomFilteringValidationState() {
        FilteringValidationState[] values = FilteringValidationState.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConfigurationDisplayType getRandomConfigurationDisplayType() {
        ConfigurationDisplayType[] values = ConfigurationDisplayType.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConfigurationFieldType getRandomConfigurationFieldType() {
        ConfigurationFieldType[] values = ConfigurationFieldType.values();
        return values[randomInt(values.length - 1)];
    }

    private static ConfigurationValidationType getRandomConfigurationValidationType() {
        ConfigurationValidationType[] values = ConfigurationValidationType.values();
        return values[randomInt(values.length - 1)];
    }
}
