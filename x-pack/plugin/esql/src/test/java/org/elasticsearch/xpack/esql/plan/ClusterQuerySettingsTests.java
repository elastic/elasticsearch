/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The operator-to-query bridge: what an operator writes into cluster settings has to reach {@link QuerySettings#resolve}.
 */
public class ClusterQuerySettingsTests extends ESTestCase {

    private static final String TIME_ZONE_KEY = "esql.query.settings.time_zone";

    private static final SettingsValidationContext CTX = new SettingsValidationContext(false, true);

    private record Fixture(ClusterQuerySettings holder, ClusterSettings clusterSettings) {}

    private static Fixture fixture(Settings nodeSettings) {
        Set<Setting<?>> registered = new HashSet<>(QuerySettings.clusterSettings());
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, registered);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new Fixture(new ClusterQuerySettings(clusterService), clusterSettings);
    }

    public void testDerivedSettingsAreRegisteredByThePlugin() {
        // Without this the two documented keys are unwritable — PUT _cluster/settings rejects them as unknown and a
        // node carrying one in elasticsearch.yml refuses to start — while every other test still passes.
        List<Setting<?>> pluginSettings = new EsqlPlugin().getSettings();
        for (Setting<?> derived : QuerySettings.clusterSettings()) {
            assertThat(pluginSettings, hasItem(derived));
        }
    }

    public void testSeedsFromNodeSettingsBeforeAnyClusterStateIsApplied() {
        // The update consumer does not fire on registration, so a value in elasticsearch.yml has to be picked up by
        // the constructor or it would not apply until something unrelated changed.
        var f = fixture(Settings.builder().put(TIME_ZONE_KEY, "Europe/Paris").build());
        assertThat(resolvedTimeZone(f.holder()), equalTo(ZoneId.of("Europe/Paris")));
    }

    public void testEmptyWhenNoOperatorValueAnywhere() {
        var f = fixture(Settings.EMPTY);
        assertThat(f.holder().values().hasValue(TIME_ZONE_KEY), equalTo(false));
        assertThat(resolvedTimeZone(f.holder()), equalTo(QuerySettings.TIME_ZONE.defaultValue()));
    }

    public void testDynamicUpdateTakesEffectWithoutRestart() {
        var f = fixture(Settings.EMPTY);
        f.clusterSettings().applySettings(Settings.builder().put(TIME_ZONE_KEY, "Asia/Tokyo").build());
        assertThat(resolvedTimeZone(f.holder()), equalTo(ZoneId.of("Asia/Tokyo")));
    }

    public void testRemovingTheClusterValueRevertsToTheNodeSetting() {
        var f = fixture(Settings.builder().put(TIME_ZONE_KEY, "Europe/Paris").build());
        f.clusterSettings().applySettings(Settings.builder().put(TIME_ZONE_KEY, "Asia/Tokyo").build());
        assertThat(resolvedTimeZone(f.holder()), equalTo(ZoneId.of("Asia/Tokyo")));

        f.clusterSettings().applySettings(Settings.EMPTY);
        assertThat(resolvedTimeZone(f.holder()), equalTo(ZoneId.of("Europe/Paris")));
    }

    public void testUnrelatedSettingsAreNotCarried() {
        var f = fixture(Settings.builder().put(TIME_ZONE_KEY, "Europe/Paris").put("esql.query.allow_partial_results", false).build());
        assertThat(f.holder().values().hasValue("esql.query.allow_partial_results"), equalTo(false));
    }

    private static ZoneId resolvedTimeZone(ClusterQuerySettings holder) {
        return QuerySettings.resolve(holder.values(), Map.of(), null, CTX).get(QuerySettings.TIME_ZONE);
    }
}
