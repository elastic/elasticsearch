/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.plugin.core.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class WatcherFeatureSetTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private NamedWriteableRegistry namedWriteableRegistry;
    private WatcherService watcherService;

    @Before
    public void init() throws Exception {
        licenseState = mock(XPackLicenseState.class);
        namedWriteableRegistry = mock(NamedWriteableRegistry.class);
        watcherService = mock(WatcherService.class);
    }

    public void testWritableRegistration() throws Exception {
        new WatcherFeatureSet(Settings.EMPTY, licenseState, namedWriteableRegistry, watcherService);
        verify(namedWriteableRegistry).register(eq(WatcherFeatureSet.Usage.class), eq("xpack.usage.watcher"), anyObject());
    }

    public void testAvailable() throws Exception {
        WatcherFeatureSet featureSet = new WatcherFeatureSet(Settings.EMPTY, licenseState, namedWriteableRegistry, watcherService);
        boolean available = randomBoolean();
        when(licenseState.isWatcherAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabled() throws Exception {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        if (enabled) {
            if (randomBoolean()) {
                settings.put("xpack.watcher.enabled", enabled);
            }
        } else {
            settings.put("xpack.watcher.enabled", enabled);
        }
        WatcherFeatureSet featureSet = new WatcherFeatureSet(settings.build(), licenseState, namedWriteableRegistry, watcherService);
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testUsageStats() throws Exception {
        Map<String, Object> statsMap = new HashMap<>();
        statsMap.put("foo", "bar");
        when(watcherService.usageStats()).thenReturn(statsMap);

        WatcherFeatureSet featureSet = new WatcherFeatureSet(Settings.EMPTY, licenseState, namedWriteableRegistry, watcherService);
        XContentBuilder builder = jsonBuilder();
        featureSet.usage().toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentSource source = new XContentSource(builder);
        assertThat(source.getValue("foo"), is("bar"));

        assertThat(featureSet.usage(), instanceOf(WatcherFeatureSet.Usage.class));
        WatcherFeatureSet.Usage usage = (WatcherFeatureSet.Usage) featureSet.usage();
        assertThat(usage.stats(), hasEntry("foo", "bar"));
    }
}
