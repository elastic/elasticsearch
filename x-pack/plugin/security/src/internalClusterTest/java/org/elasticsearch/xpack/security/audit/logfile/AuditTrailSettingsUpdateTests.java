/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ClusterScope(scope = TEST, numDataNodes = 1)
public class AuditTrailSettingsUpdateTests extends SecurityIntegTestCase {

    private static Settings startupFilterSettings;
    private static Settings updateFilterSettings;

    @BeforeClass
    public static void startupFilterSettings() {
        final Settings.Builder settingsBuilder = Settings.builder();
        // generate random filter policies
        for (int i = 0; i < randomIntBetween(0, 4); i++) {
            settingsBuilder.put(randomFilterPolicySettings("startupPolicy" + i));
        }
        startupFilterSettings = settingsBuilder.build();
    }

    @BeforeClass
    public static void updateFilterSettings() {
        final Settings.Builder settingsBuilder = Settings.builder();
        // generate random filter policies
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            settingsBuilder.put(randomFilterPolicySettings("updatePolicy" + i));
        }
        updateFilterSettings = settingsBuilder.build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(super.nodeSettings(nodeOrdinal, otherSettings));

        // enable auditing
        settingsBuilder.put("xpack.security.audit.enabled", "true");
        // add only startup filter policies
        settingsBuilder.put(startupFilterSettings);
        return settingsBuilder.build();
    }

    public void testDynamicFilterSettings() throws Exception {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterSettings clusterSettings = mockClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(startupFilterSettings);
        settingsBuilder.put(updateFilterSettings);
        // reference audit trail containing all filters
        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settingsBuilder.build(), clusterService, logger, threadContext);
        final String expected = auditTrail.eventFilterPolicyRegistry.toString();
        // update settings on internal cluster
        updateClusterSettings(Settings.builder().put(updateFilterSettings));
        final String actual = ((LoggingAuditTrail) internalCluster().getInstances(AuditTrailService.class)
            .iterator()
            .next()
            .getAuditTrail()).eventFilterPolicyRegistry.toString();
        assertEquals(expected, actual);
    }

    public void testInvalidFilterSettings() throws Exception {
        final String invalidLuceneRegex = "/invalid";
        final Settings.Builder settingsBuilder = Settings.builder();
        final String[] allSettingsKeys = new String[] {
            "xpack.security.audit.logfile.events.ignore_filters.invalid.users",
            "xpack.security.audit.logfile.events.ignore_filters.invalid.realms",
            "xpack.security.audit.logfile.events.ignore_filters.invalid.roles",
            "xpack.security.audit.logfile.events.ignore_filters.invalid.indices",
            "xpack.security.audit.logfile.events.ignore_filters.invalid.actions" };
        settingsBuilder.put(randomFrom(allSettingsKeys), invalidLuceneRegex);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> clusterAdmin().prepareUpdateSettings().setPersistentSettings(settingsBuilder.build()).get()
        );
        assertThat(e.getMessage(), containsString("invalid pattern [/invalid]"));
    }

    public void testDynamicHostSettings() {
        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING.getKey(), true);
        settingsBuilder.put(LoggingAuditTrail.EMIT_HOST_NAME_SETTING.getKey(), true);
        settingsBuilder.put(LoggingAuditTrail.EMIT_NODE_NAME_SETTING.getKey(), true);
        settingsBuilder.put(LoggingAuditTrail.EMIT_NODE_ID_SETTING.getKey(), true);
        updateClusterSettings(settingsBuilder);
        final LoggingAuditTrail loggingAuditTrail = (LoggingAuditTrail) internalCluster().getInstances(AuditTrailService.class)
            .iterator()
            .next()
            .getAuditTrail();
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.NODE_NAME_FIELD_NAME), startsWith("node_"));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.containsKey(LoggingAuditTrail.NODE_ID_FIELD_NAME), is(true));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_ADDRESS_FIELD_NAME), is("127.0.0.1"));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_NAME_FIELD_NAME), is("127.0.0.1"));
        settingsBuilder.put(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING.getKey(), false);
        updateClusterSettings(settingsBuilder);
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.NODE_NAME_FIELD_NAME), startsWith("node_"));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.containsKey(LoggingAuditTrail.NODE_ID_FIELD_NAME), is(true));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_ADDRESS_FIELD_NAME), is(nullValue()));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_NAME_FIELD_NAME), is("127.0.0.1"));
        settingsBuilder.put(LoggingAuditTrail.EMIT_HOST_NAME_SETTING.getKey(), false);
        updateClusterSettings(settingsBuilder);
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.NODE_NAME_FIELD_NAME), startsWith("node_"));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.containsKey(LoggingAuditTrail.NODE_ID_FIELD_NAME), is(true));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_ADDRESS_FIELD_NAME), is(nullValue()));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_NAME_FIELD_NAME), is(nullValue()));
        settingsBuilder.put(LoggingAuditTrail.EMIT_NODE_NAME_SETTING.getKey(), false);
        updateClusterSettings(settingsBuilder);
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.NODE_NAME_FIELD_NAME), is(nullValue()));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.containsKey(LoggingAuditTrail.NODE_ID_FIELD_NAME), is(true));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_ADDRESS_FIELD_NAME), is(nullValue()));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_NAME_FIELD_NAME), is(nullValue()));
        settingsBuilder.put(LoggingAuditTrail.EMIT_NODE_ID_SETTING.getKey(), false);
        updateClusterSettings(settingsBuilder);
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.NODE_NAME_FIELD_NAME), is(nullValue()));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.NODE_ID_FIELD_NAME), is(nullValue()));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_ADDRESS_FIELD_NAME), is(nullValue()));
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.HOST_NAME_FIELD_NAME), is(nullValue()));
    }

    public void testDynamicClusterSettings() {
        final LoggingAuditTrail loggingAuditTrail = (LoggingAuditTrail) internalCluster().getInstances(AuditTrailService.class)
            .iterator()
            .next()
            .getAuditTrail();

        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(LoggingAuditTrail.EMIT_CLUSTER_NAME_SETTING.getKey(), true);
        settingsBuilder.put(LoggingAuditTrail.EMIT_CLUSTER_UUID_SETTING.getKey(), true);
        updateClusterSettings(settingsBuilder);
        final String clusterName = loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.CLUSTER_NAME_FIELD_NAME);
        final String clusterUuid = loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.CLUSTER_UUID_FIELD_NAME);
        assertThat(clusterName, not(emptyOrNullString()));
        assertThat(clusterUuid, not(emptyOrNullString()));

        settingsBuilder.put(LoggingAuditTrail.EMIT_CLUSTER_NAME_SETTING.getKey(), false);
        updateClusterSettings(settingsBuilder);
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.CLUSTER_NAME_FIELD_NAME), is(nullValue()));

        settingsBuilder.put(LoggingAuditTrail.EMIT_CLUSTER_NAME_SETTING.getKey(), true);
        updateClusterSettings(settingsBuilder);
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.CLUSTER_NAME_FIELD_NAME), is(clusterName));

        settingsBuilder.put(LoggingAuditTrail.EMIT_CLUSTER_UUID_SETTING.getKey(), false);
        updateClusterSettings(settingsBuilder);
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.CLUSTER_UUID_FIELD_NAME), is(nullValue()));

        settingsBuilder.put(LoggingAuditTrail.EMIT_CLUSTER_UUID_SETTING.getKey(), true);
        updateClusterSettings(settingsBuilder);
        assertThat(loggingAuditTrail.entryCommonFields.commonFields.get(LoggingAuditTrail.CLUSTER_UUID_FIELD_NAME), is(clusterUuid));
    }

    public void testDynamicRequestBodySettings() {
        final boolean enableRequestBody = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(LoggingAuditTrail.INCLUDE_REQUEST_BODY.getKey(), enableRequestBody);
        updateClusterSettings(settingsBuilder);
        final LoggingAuditTrail loggingAuditTrail = (LoggingAuditTrail) internalCluster().getInstances(AuditTrailService.class)
            .iterator()
            .next()
            .getAuditTrail();
        assertEquals(enableRequestBody, loggingAuditTrail.includeRequestBody);
        settingsBuilder.put(LoggingAuditTrail.INCLUDE_REQUEST_BODY.getKey(), enableRequestBody == false);
        updateClusterSettings(settingsBuilder);
        assertEquals(enableRequestBody == false, loggingAuditTrail.includeRequestBody);
    }

    public void testDynamicEventsSettings() {
        final List<String> allEventTypes = Arrays.asList(
            "anonymous_access_denied",
            "authentication_failed",
            "realm_authentication_failed",
            "access_granted",
            "access_denied",
            "tampered_request",
            "connection_granted",
            "connection_denied",
            "system_access_granted",
            "authentication_success",
            "run_as_granted",
            "run_as_denied"
        );
        final List<String> includedEvents = randomSubsetOf(allEventTypes);
        final List<String> excludedEvents = randomSubsetOf(allEventTypes);
        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.putList(LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.getKey(), includedEvents);
        settingsBuilder.putList(LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS.getKey(), excludedEvents);
        updateClusterSettings(settingsBuilder);
        final LoggingAuditTrail loggingAuditTrail = (LoggingAuditTrail) internalCluster().getInstances(AuditTrailService.class)
            .iterator()
            .next()
            .getAuditTrail();
        assertEquals(AuditLevel.parse(includedEvents, excludedEvents), loggingAuditTrail.events);
    }

    private static List<String> randomNonEmptyListOfFilteredNames(String... namePrefix) {
        final List<String> filtered = new ArrayList<>(4);
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            filtered.add(Strings.arrayToCommaDelimitedString(namePrefix) + randomAlphaOfLengthBetween(1, 4));
        }
        return filtered;
    }

    private static Settings randomFilterPolicySettings(String policyName) {
        final Settings.Builder settingsBuilder = Settings.builder();
        do {
            if (randomBoolean()) {
                // filter by username
                final List<String> filteredUsernames = randomNonEmptyListOfFilteredNames();
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters." + policyName + ".users", filteredUsernames);
            }
            if (randomBoolean()) {
                // filter by realms
                final List<String> filteredRealms = randomNonEmptyListOfFilteredNames();
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters." + policyName + ".realms", filteredRealms);
            }
            if (randomBoolean()) {
                // filter by roles
                final List<String> filteredRoles = randomNonEmptyListOfFilteredNames();
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters." + policyName + ".roles", filteredRoles);
            }
            if (randomBoolean()) {
                // filter by indices
                final List<String> filteredIndices = randomNonEmptyListOfFilteredNames();
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters." + policyName + ".indices", filteredIndices);
            }
            if (randomBoolean()) {
                // filter by actions
                final List<String> filteredActions = randomNonEmptyListOfFilteredNames();
                settingsBuilder.putList("xpack.security.audit.logfile.events.ignore_filters." + policyName + ".actions", filteredActions);
            }
        } while (settingsBuilder.build().isEmpty());

        assertFalse(settingsBuilder.build().isEmpty());

        return settingsBuilder.build();
    }

    private ClusterSettings mockClusterSettings() {
        final List<Setting<?>> settingsList = new ArrayList<>();
        LoggingAuditTrail.registerSettings(settingsList);
        settingsList.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new ClusterSettings(Settings.EMPTY, new HashSet<>(settingsList));
    }
}
