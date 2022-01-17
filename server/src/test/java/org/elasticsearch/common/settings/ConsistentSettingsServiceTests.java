/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ConsistentSettingsServiceTests extends ESTestCase {

    private AtomicReference<ClusterState> clusterState = new AtomicReference<>();
    private ClusterService clusterService;

    @Before
    public void init() throws Exception {
        clusterState.set(ClusterState.EMPTY_STATE);
        clusterService = mock(ClusterService.class);
        Mockito.doAnswer((Answer) invocation -> { return clusterState.get(); }).when(clusterService).state();
        Mockito.doAnswer((Answer) invocation -> {
            final ClusterStateUpdateTask arg0 = (ClusterStateUpdateTask) invocation.getArguments()[1];
            this.clusterState.set(arg0.execute(this.clusterState.get()));
            return null;
        }).when(clusterService).submitStateUpdateTask(Mockito.isA(String.class), Mockito.isA(ClusterStateUpdateTask.class), Mockito.any());
    }

    public void testSingleStringSetting() throws Exception {
        Setting<?> stringSetting = SecureSetting.secureString("test.simple.foo", null, Setting.Property.Consistent);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(stringSetting.getKey(), "somethingsecure");
        secureSettings.setString("test.noise.setting", "noise");
        Settings.Builder builder = Settings.builder();
        builder.setSecureSettings(secureSettings);
        Settings settings = builder.build();
        // hashes not yet published
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).areAllConsistent(), is(false));
        // publish
        new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).newHashPublisher().onMaster();
        ConsistentSettingsService consistentService = new ConsistentSettingsService(settings, clusterService, List.of(stringSetting));
        assertThat(consistentService.areAllConsistent(), is(true));
        // change value
        secureSettings.setString(stringSetting.getKey(), "_TYPO_somethingsecure");
        assertThat(consistentService.areAllConsistent(), is(false));
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).areAllConsistent(), is(false));
        // publish change
        new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).newHashPublisher().onMaster();
        assertThat(consistentService.areAllConsistent(), is(true));
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).areAllConsistent(), is(true));
    }

    public void testSingleAffixSetting() throws Exception {
        Setting.AffixSetting<?> affixStringSetting = Setting.affixKeySetting(
            "test.affix.",
            "bar",
            (key) -> SecureSetting.secureString(key, null, Setting.Property.Consistent)
        );
        // add two affix settings to the keystore
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("test.noise.setting", "noise");
        secureSettings.setString("test.affix.first.bar", "first_secure");
        secureSettings.setString("test.affix.second.bar", "second_secure");
        Settings.Builder builder = Settings.builder();
        builder.setSecureSettings(secureSettings);
        Settings settings = builder.build();
        // hashes not yet published
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(false));
        // publish
        new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).newHashPublisher().onMaster();
        ConsistentSettingsService consistentService = new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting));
        assertThat(consistentService.areAllConsistent(), is(true));
        // change value
        secureSettings.setString("test.affix.second.bar", "_TYPO_second_secure");
        assertThat(consistentService.areAllConsistent(), is(false));
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(false));
        // publish change
        new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).newHashPublisher().onMaster();
        assertThat(consistentService.areAllConsistent(), is(true));
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(true));
        // add value
        secureSettings.setString("test.affix.third.bar", "third_secure");
        builder = Settings.builder();
        builder.setSecureSettings(secureSettings);
        settings = builder.build();
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(false));
        // publish
        new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).newHashPublisher().onMaster();
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(true));
        // remove value
        secureSettings = new MockSecureSettings();
        secureSettings.setString("test.another.noise.setting", "noise");
        // missing value test.affix.first.bar
        secureSettings.setString("test.affix.second.bar", "second_secure");
        secureSettings.setString("test.affix.third.bar", "third_secure");
        builder = Settings.builder();
        builder.setSecureSettings(secureSettings);
        settings = builder.build();
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(false));
    }

    public void testStringAndAffixSettings() throws Exception {
        Setting<?> stringSetting = SecureSetting.secureString("mock.simple.foo", null, Setting.Property.Consistent);
        Setting.AffixSetting<?> affixStringSetting = Setting.affixKeySetting(
            "mock.affix.",
            "bar",
            (key) -> SecureSetting.secureString(key, null, Setting.Property.Consistent)
        );
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(randomAlphaOfLength(8).toLowerCase(Locale.ROOT), "noise");
        secureSettings.setString(stringSetting.getKey(), "somethingsecure");
        secureSettings.setString("mock.affix.foo.bar", "another_secure");
        Settings.Builder builder = Settings.builder();
        builder.setSecureSettings(secureSettings);
        Settings settings = builder.build();
        // hashes not yet published
        assertThat(
            new ConsistentSettingsService(settings, clusterService, List.of(stringSetting, affixStringSetting)).areAllConsistent(),
            is(false)
        );
        // publish only the simple string setting
        new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).newHashPublisher().onMaster();
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).areAllConsistent(), is(true));
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(false));
        assertThat(
            new ConsistentSettingsService(settings, clusterService, List.of(stringSetting, affixStringSetting)).areAllConsistent(),
            is(false)
        );
        // publish only the affix string setting
        new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).newHashPublisher().onMaster();
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).areAllConsistent(), is(false));
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(true));
        assertThat(
            new ConsistentSettingsService(settings, clusterService, List.of(stringSetting, affixStringSetting)).areAllConsistent(),
            is(false)
        );
        // publish both settings
        new ConsistentSettingsService(settings, clusterService, List.of(stringSetting, affixStringSetting)).newHashPublisher().onMaster();
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(stringSetting)).areAllConsistent(), is(true));
        assertThat(new ConsistentSettingsService(settings, clusterService, List.of(affixStringSetting)).areAllConsistent(), is(true));
        assertThat(
            new ConsistentSettingsService(settings, clusterService, List.of(stringSetting, affixStringSetting)).areAllConsistent(),
            is(true)
        );
    }
}
