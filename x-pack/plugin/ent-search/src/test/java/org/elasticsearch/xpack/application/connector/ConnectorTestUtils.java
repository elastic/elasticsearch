/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;
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
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidation;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationInfo;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationState;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobType;
import org.elasticsearch.xpack.core.scheduler.Cron;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.CONNECTOR_INDEX_NAME_PATTERN;
import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN;
import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_TEMPLATE_NAME;
import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME;

public final class ConnectorTestUtils {

    public static final String NULL_STRING = null;

    /**
     * Registers index templates for instances of {@link Connector} and {@link ConnectorSyncJob} with essential field mappings. This method
     * only includes mappings for fields relevant to test cases, specifying field types to ensure correct ES query logic behavior.
     *
     * @param indicesAdminClient The Elasticsearch indices admin client used for template registration.
     */

    public static void registerSimplifiedConnectorIndexTemplates(IndicesAdminClient indicesAdminClient) {

        indicesAdminClient.preparePutTemplate(CONNECTOR_TEMPLATE_NAME)
            .setPatterns(List.of(CONNECTOR_INDEX_NAME_PATTERN))
            .setVersion(0)
            .setMapping(
                "service_type",
                "type=keyword,store=true",
                "status",
                "type=keyword,store=true",
                "index_name",
                "type=keyword,store=true",
                "configuration",
                "type=object"
            )
            .get();

        indicesAdminClient.preparePutTemplate(CONNECTOR_SYNC_JOBS_TEMPLATE_NAME)
            .setPatterns(List.of(CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN))
            .setVersion(0)
            .setMapping(
                "job_type",
                "type=keyword,store=true",
                "connector.id",
                "type=keyword,store=true",
                "status",
                "type=keyword,store=true"
            )
            .get();
    }

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
            .setLastAccessControlSyncScheduledAt(randomFrom(new Instant[] { null, ConnectorTestUtils.randomInstant() }))
            .setLastAccessControlSyncStatus(randomFrom(new ConnectorSyncStatus[] { null, getRandomSyncStatus() }))
            .setLastDeletedDocumentCount(randomLong())
            .setLastIncrementalSyncScheduledAt(randomFrom(new Instant[] { null, ConnectorTestUtils.randomInstant() }))
            .setLastIndexedDocumentCount(randomLong())
            .setLastSyncError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastSyncScheduledAt(randomFrom(new Instant[] { null, ConnectorTestUtils.randomInstant() }))
            .setLastSyncStatus(randomFrom(new ConnectorSyncStatus[] { null, getRandomSyncStatus() }))
            .setLastSynced(randomFrom(new Instant[] { null, ConnectorTestUtils.randomInstant() }))
            .build();
    }

    public static ConnectorFeatures getRandomConnectorFeatures() {
        return new ConnectorFeatures.Builder().setDocumentLevelSecurityEnabled(randomBoolean() ? randomConnectorFeatureEnabled() : null)
            .setIncrementalSyncEnabled(randomBoolean() ? randomConnectorFeatureEnabled() : null)
            .setNativeConnectorAPIKeysEnabled(randomBoolean() ? randomConnectorFeatureEnabled() : null)
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

    public static FilteringValidationInfo getRandomFilteringValidationInfo() {
        return new FilteringValidationInfo.Builder().setValidationErrors(getRandomFilteringValidationErrors())
            .setValidationState(getRandomFilteringValidationState())
            .build();
    }

    private static List<FilteringValidation> getRandomFilteringValidationErrors() {
        return List.of(getRandomFilteringValidationError(), getRandomFilteringValidationError(), getRandomFilteringValidationError());
    }

    private static FilteringValidation getRandomFilteringValidationError() {
        return new FilteringValidation.Builder().setIds(List.of(randomAlphaOfLength(5), randomAlphaOfLength(5)))
            .setMessages(List.of(randomAlphaOfLengthBetween(10, 20), randomAlphaOfLengthBetween(15, 25)))
            .build();
    }

    public static ConnectorFiltering getRandomConnectorFiltering() {

        Instant currentTimestamp = Instant.now();
        int order = randomInt();

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
                            .setOrder(order)
                            .setPolicy(getRandomFilteringPolicy())
                            .setRule(getRandomFilteringRuleCondition())
                            .setUpdatedAt(currentTimestamp)
                            .setValue(randomAlphaOfLength(10))
                            .build(),
                        ConnectorFiltering.getDefaultFilteringRule(currentTimestamp, order + 1)
                    )
                )
                .setFilteringValidationInfo(getRandomFilteringValidationInfo())
                .build()
        )
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
                                .setOrder(order)
                                .setPolicy(getRandomFilteringPolicy())
                                .setRule(getRandomFilteringRuleCondition())
                                .setUpdatedAt(currentTimestamp)
                                .setValue(randomAlphaOfLength(10))
                                .build(),
                            ConnectorFiltering.getDefaultFilteringRule(currentTimestamp, order + 1)

                        )
                    )
                    .setFilteringValidationInfo(getRandomFilteringValidationInfo())
                    .build()
            )
            .build();
    }

    public static Connector getRandomSyncJobConnectorInfo() {
        ConnectorFiltering randomFiltering = getRandomConnectorFiltering();
        return new Connector.Builder().setConnectorId(randomAlphaOfLength(10))
            .setSyncJobFiltering(randomFiltering.getActive())
            .setFiltering(List.of(randomFiltering))
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
        for (int i = 0; i < 5; i++) {
            configMap.put(randomAlphaOfLength(10), getRandomConnectorConfigurationField());
        }
        return configMap;
    }

    public static Map<String, Object> getRandomConnectorConfigurationValues() {
        Map<String, Object> configMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            configMap.put(randomAlphaOfLength(10), randomFrom(randomAlphaOfLengthBetween(3, 10), randomInt(), randomBoolean()));
        }
        return configMap;
    }

    private static Connector.Builder getRandomConnectorBuilder() {
        return new Connector.Builder().setApiKeyId(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setApiKeySecretId(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setConfiguration(getRandomConnectorConfiguration())
            .setCustomScheduling(Map.of(randomAlphaOfLengthBetween(5, 10), getRandomConnectorCustomSchedule()))
            .setDescription(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setFeatures(randomBoolean() ? getRandomConnectorFeatures() : null)
            .setFiltering(List.of(getRandomConnectorFiltering()))
            .setIndexName(randomAlphaOfLength(10))
            .setIsNative(randomBoolean())
            .setLanguage(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setLastSeen(randomFrom(new Instant[] { null, ConnectorTestUtils.randomInstant() }))
            .setSyncInfo(getRandomConnectorSyncInfo())
            .setName(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setPipeline(randomBoolean() ? getRandomConnectorIngestPipeline() : null)
            .setServiceType(randomAlphaOfLengthBetween(5, 10))
            .setScheduling(getRandomConnectorScheduling())
            .setStatus(getRandomConnectorInitialStatus())
            .setSyncCursor(randomBoolean() ? Map.of(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)) : null)
            .setSyncNow(randomBoolean());
    }

    public static Connector getRandomConnector() {
        return getRandomConnectorBuilder().build();
    }

    public static Connector getRandomConnectorWithDetachedIndex() {
        return getRandomConnectorBuilder().setIndexName(null).build();
    }

    public static Connector getRandomConnectorWithAttachedIndex(String indexName) {
        return getRandomConnectorBuilder().setIndexName(indexName).build();
    }

    public static Connector getRandomSelfManagedConnector() {
        return getRandomConnectorBuilder().setIsNative(false).build();
    }

    public static Connector getRandomElasticManagedConnector() {
        return getRandomConnectorBuilder().setIsNative(true).build();
    }

    public static Connector getRandomConnectorWithServiceTypeNotDefined() {
        return getRandomConnectorBuilder().setServiceType(null).build();
    }

    private static BytesReference convertConnectorToBytesReference(Connector connector) {
        try {
            return XContentHelper.toXContent((builder, params) -> {
                connector.toInnerXContent(builder, params);
                return builder;
            }, XContentType.JSON, null, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Object> convertConnectorToGenericMap(Connector connector) {
        return XContentHelper.convertToMap(convertConnectorToBytesReference(connector), true, XContentType.JSON).v2();
    }

    public static ConnectorSearchResult getRandomConnectorSearchResult() {
        Connector connector = getRandomConnector();

        return new ConnectorSearchResult.Builder().setResultBytes(convertConnectorToBytesReference(connector))
            .setResultMap(convertConnectorToGenericMap(connector))
            .setId(randomAlphaOfLength(10))
            .build();
    }

    public static ConnectorFeatures.FeatureEnabled randomConnectorFeatureEnabled() {
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
    public static Cron getRandomCronExpression() {
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

    /**
     * Generate a random Instant between:
     * - 1 January 1970 00:00:00+00:00
     * - 24 January 2065 05:20:00+00:00
     */
    public static Instant randomInstant() {
        Instant lowerBoundInstant = Instant.ofEpochSecond(0L);
        Instant upperBoundInstant = Instant.ofEpochSecond(3000000000L);

        return Instant.ofEpochSecond(
            randomLongBetween(lowerBoundInstant.getEpochSecond(), upperBoundInstant.getEpochSecond()),
            randomLongBetween(0, 999999999)
        );
    }

    public static ConnectorSyncStatus getRandomSyncStatus() {
        ConnectorSyncStatus[] values = ConnectorSyncStatus.values();
        return values[randomInt(values.length - 1)];
    }

    public static ConnectorSyncJobType getRandomSyncJobType() {
        ConnectorSyncJobType[] values = ConnectorSyncJobType.values();
        return values[randomInt(values.length - 1)];
    }

    public static ConnectorStatus getRandomConnectorInitialStatus() {
        return randomFrom(ConnectorStatus.CREATED, ConnectorStatus.NEEDS_CONFIGURATION);
    }

    public static ConnectorStatus getRandomConnectorNextStatus(ConnectorStatus connectorStatus) {
        return randomFrom(ConnectorStateMachine.validNextStates(connectorStatus));
    }

    public static ConnectorStatus getRandomInvalidConnectorNextStatus(ConnectorStatus connectorStatus) {
        Set<ConnectorStatus> validNextStatus = ConnectorStateMachine.validNextStates(connectorStatus);
        List<ConnectorStatus> invalidStatuses = Arrays.stream(ConnectorStatus.values())
            .filter(status -> validNextStatus.contains(status) == false)
            .toList();
        return randomFrom(invalidStatuses);
    }

    public static ConnectorStatus getRandomConnectorStatus() {
        ConnectorStatus[] values = ConnectorStatus.values();
        return values[randomInt(values.length - 1)];
    }

    public static FilteringPolicy getRandomFilteringPolicy() {
        FilteringPolicy[] values = FilteringPolicy.values();
        return values[randomInt(values.length - 1)];
    }

    public static FilteringRuleCondition getRandomFilteringRuleCondition() {
        FilteringRuleCondition[] values = FilteringRuleCondition.values();
        return values[randomInt(values.length - 1)];
    }

    public static FilteringValidationState getRandomFilteringValidationState() {
        FilteringValidationState[] values = FilteringValidationState.values();
        return values[randomInt(values.length - 1)];
    }

    public static ConfigurationDisplayType getRandomConfigurationDisplayType() {
        ConfigurationDisplayType[] values = ConfigurationDisplayType.values();
        return values[randomInt(values.length - 1)];
    }

    public static ConfigurationFieldType getRandomConfigurationFieldType() {
        ConfigurationFieldType[] values = ConfigurationFieldType.values();
        return values[randomInt(values.length - 1)];
    }

    public static ConfigurationValidationType getRandomConfigurationValidationType() {
        ConfigurationValidationType[] values = ConfigurationValidationType.values();
        return values[randomInt(values.length - 1)];
    }
}
