/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PreferIlmSettingProviderTests extends ESTestCase {

    private static final String WARM_PHASE = "warm";
    private static final String METRICS_POLICY_NAME = "metrics";
    private static final String DATA_STREAM_NAME = "metrics-apache.access-default";
    private static final String INDEX_NAME = ".ds-metrics-apache.access-default-2026.10.06-000001";

    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(PreferIlmSettingProvider.ENABLED_SETTING));
    private final PreferIlmSettingProvider provider = PreferIlmSettingProvider.create(clusterSettings);

    public void testPreferIlmDefaultsToFalse() {
        ProjectMetadata project = projectWithPolicies(onlyRolloverPolicyMetadata(METRICS_POLICY_NAME, 1L));

        Settings templateSettings = resolvedSettings(METRICS_POLICY_NAME, null);
        Settings.Builder additionalSettings = Settings.builder();
        provider.provideAdditionalSettings(
            INDEX_NAME,
            DATA_STREAM_NAME,
            IndexMode.TIME_SERIES,
            randomBoolean(),
            project,
            Instant.now(),
            templateSettings,
            List.of(),
            IndexVersion.current(),
            additionalSettings
        );
        assertThat(additionalSettings.keys().contains(IndexSettings.PREFER_ILM), equalTo(true));
        assertThat(additionalSettings.get(IndexSettings.PREFER_ILM), equalTo("false"));
    }

    public void testPreferIlmRemainsTheSameIfTheIndexIsNotACandidate() {
        ProjectMetadata project = projectWithPolicies(onlyRolloverPolicyMetadata(METRICS_POLICY_NAME, 1L));
        Settings compatibleSettings = resolvedSettings(METRICS_POLICY_NAME, null);
        // When there is no data stream
        {

            Settings.Builder additionalSettings = Settings.builder();
            provider.provideAdditionalSettings(
                INDEX_NAME,
                null,
                randomFrom(IndexMode.TIME_SERIES),
                randomBoolean(),
                project,
                Instant.now(),
                compatibleSettings,
                List.of(),
                IndexVersion.current(),
                additionalSettings
            );
            assertThat(additionalSettings.build(), equalTo(Settings.EMPTY));
        }

        // When it's not time series
        {
            Settings.Builder additionalSettings = Settings.builder();
            provider.provideAdditionalSettings(
                INDEX_NAME,
                DATA_STREAM_NAME,
                randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB, IndexMode.LOGSDB_COLUMNAR, null),
                randomBoolean(),
                project,
                Instant.now(),
                compatibleSettings,
                List.of(),
                IndexVersion.current(),
                additionalSettings
            );
            assertThat(additionalSettings.build(), equalTo(Settings.EMPTY));
        }

        // When prefer_ilm is set
        {
            Settings preferIlmSet = resolvedSettings(METRICS_POLICY_NAME, randomBoolean());
            Settings.Builder additionalSettings = Settings.builder();
            provider.provideAdditionalSettings(
                INDEX_NAME,
                DATA_STREAM_NAME,
                randomFrom(IndexMode.TIME_SERIES),
                randomBoolean(),
                project,
                Instant.now(),
                preferIlmSet,
                List.of(),
                IndexVersion.current(),
                additionalSettings
            );
            assertThat(additionalSettings.build(), equalTo(Settings.EMPTY));
        }
    }

    public void testPreferIlmRemainsTheSameIfNotManagedByDefaultPolicy() {
        String otherPolicyName = "other-policy";

        // Managed by another policy
        {
            ProjectMetadata project = projectWithPolicies(
                onlyRolloverPolicyMetadata(METRICS_POLICY_NAME, 1L),
                someOtherPolicyMetadata(otherPolicyName, 1L)
            );
            Settings templateSettings = resolvedSettings(otherPolicyName, null);
            Settings.Builder additionalSettings = Settings.builder();
            provider.provideAdditionalSettings(
                INDEX_NAME,
                DATA_STREAM_NAME,
                IndexMode.TIME_SERIES,
                randomBoolean(),
                project,
                Instant.now(),
                templateSettings,
                List.of(),
                IndexVersion.current(),
                additionalSettings
            );
            assertThat(additionalSettings.build(), equalTo(Settings.EMPTY));
        }

        // Default policy with different version
        {
            ProjectMetadata project = projectWithPolicies(onlyRolloverPolicyMetadata(METRICS_POLICY_NAME, 2L));
            Settings.Builder additionalSettings = Settings.builder();
            provider.provideAdditionalSettings(
                INDEX_NAME,
                DATA_STREAM_NAME,
                IndexMode.TIME_SERIES,
                randomBoolean(),
                project,
                Instant.now(),
                Settings.EMPTY,
                List.of(),
                IndexVersion.current(),
                additionalSettings
            );
            assertThat(additionalSettings.build(), equalTo(Settings.EMPTY));
        }

        // Default policy installed by the user
        {
            ProjectMetadata project = projectWithPolicies(someOtherPolicyMetadata(METRICS_POLICY_NAME, 1L));
            Settings.Builder additionalSettings = Settings.builder();
            provider.provideAdditionalSettings(
                INDEX_NAME,
                DATA_STREAM_NAME,
                IndexMode.TIME_SERIES,
                randomBoolean(),
                project,
                Instant.now(),
                Settings.EMPTY,
                List.of(),
                IndexVersion.current(),
                additionalSettings
            );
            assertThat(additionalSettings.build(), equalTo(Settings.EMPTY));
        }
    }

    public void testEnabledByDefault() {
        assertThat(PreferIlmSettingProvider.ENABLED_SETTING.get(Settings.EMPTY), is(true));
        assertThat(PreferIlmSettingProvider.ENABLED_SETTING.isDynamic(), is(true));
    }

    public void testCanBeDisabledAndReEnabledDynamically() {
        ProjectMetadata project = projectWithPolicies(onlyRolloverPolicyMetadata(METRICS_POLICY_NAME, 1L));
        assertThat(preferIlmProvided(provider, project), is(true));

        clusterSettings.applySettings(Settings.builder().put(PreferIlmSettingProvider.ENABLED_SETTING.getKey(), false).build());
        assertThat(preferIlmProvided(provider, project), is(false));

        clusterSettings.applySettings(Settings.builder().put(PreferIlmSettingProvider.ENABLED_SETTING.getKey(), true).build());
        assertThat(preferIlmProvided(provider, project), is(true));
    }

    public void testDisabledAtConstruction() {
        ClusterSettings disabled = new ClusterSettings(
            Settings.builder().put(PreferIlmSettingProvider.ENABLED_SETTING.getKey(), false).build(),
            Set.of(PreferIlmSettingProvider.ENABLED_SETTING)
        );
        ProjectMetadata project = projectWithPolicies(onlyRolloverPolicyMetadata(METRICS_POLICY_NAME, 1L));
        assertThat(preferIlmProvided(PreferIlmSettingProvider.create(disabled), project), is(false));
    }

    private static boolean preferIlmProvided(PreferIlmSettingProvider provider, ProjectMetadata project) {
        Settings.Builder additionalSettings = Settings.builder();
        provider.provideAdditionalSettings(
            INDEX_NAME,
            DATA_STREAM_NAME,
            IndexMode.TIME_SERIES,
            randomBoolean(),
            project,
            Instant.now(),
            resolvedSettings(METRICS_POLICY_NAME, null),
            List.of(),
            IndexVersion.current(),
            additionalSettings
        );
        return additionalSettings.keys().contains(IndexSettings.PREFER_ILM);
    }

    /**
     * The provider supplies a default only. An explicit value in the template or the create index request is applied on
     * top of it by {@code MetadataCreateIndexService}, so the provider must not claim to overrule those settings.
     */
    public void testDoesNotOverruleTemplateAndRequestSettings() {
        assertThat(provider.overrulesTemplateAndRequestSettings(), is(false));
    }

    private static Settings resolvedSettings(String policyName, Boolean preferIlm) {
        Settings.Builder builder = Settings.builder();
        if (policyName != null) {
            builder.put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        }
        if (preferIlm != null) {
            builder.put(IndexSettings.PREFER_ILM, preferIlm);
        }
        return builder.build();
    }

    private static ProjectMetadata projectWithPolicies(LifecyclePolicyMetadata... policyMetadata) {
        Map<String, LifecyclePolicyMetadata> policies = new HashMap<>();
        for (LifecyclePolicyMetadata policy : policyMetadata) {
            policies.put(policy.getName(), policy);
        }
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(policies, OperationMode.RUNNING);
        return ProjectMetadata.builder(randomProjectIdOrDefault()).putCustom(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata).build();
    }

    private static LifecyclePolicyMetadata onlyRolloverPolicyMetadata(String policyName, long version) {
        LifecyclePolicy policy = new LifecyclePolicy(
            TimeseriesLifecycleType.INSTANCE,
            policyName,
            Map.of(
                TimeseriesLifecycleType.HOT_PHASE,
                new Phase(TimeseriesLifecycleType.HOT_PHASE, TimeValue.ZERO, Map.of(RolloverAction.NAME, rolloverAction()))
            ),
            Map.of(),
            null
        );
        return new LifecyclePolicyMetadata(policy, Map.of(), version, randomNonNegativeLong());
    }

    private static LifecyclePolicyMetadata someOtherPolicyMetadata(String policyName, long version) {
        LifecyclePolicy policy = new LifecyclePolicy(
            TimeseriesLifecycleType.INSTANCE,
            policyName,
            Map.of(
                TimeseriesLifecycleType.HOT_PHASE,
                new Phase(TimeseriesLifecycleType.HOT_PHASE, TimeValue.ZERO, Map.of(RolloverAction.NAME, rolloverAction())),
                WARM_PHASE,
                new Phase(WARM_PHASE, TimeValue.ZERO, Map.of(SetPriorityAction.NAME, new SetPriorityAction(100)))
            ),
            Map.of(),
            null
        );
        return new LifecyclePolicyMetadata(policy, Map.of(), version, randomNonNegativeLong());
    }

    private static RolloverAction rolloverAction() {
        return new RolloverAction(null, ByteSizeValue.ofGb(50), TimeValue.timeValueDays(30), null, null, null, null, null, null, null);
    }
}
