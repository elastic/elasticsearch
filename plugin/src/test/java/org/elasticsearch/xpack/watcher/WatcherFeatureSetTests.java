/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.junit.Before;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WatcherFeatureSetTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private WatcherService watcherService;

    @Before
    public void init() throws Exception {
        licenseState = mock(XPackLicenseState.class);
        watcherService = mock(WatcherService.class);
    }

    public void testAvailable() throws Exception {
        WatcherFeatureSet featureSet = new WatcherFeatureSet(Settings.EMPTY, licenseState, watcherService);
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
        WatcherFeatureSet featureSet = new WatcherFeatureSet(settings.build(), licenseState, watcherService);
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testUsageStats() throws Exception {
        Map<String, Object> statsMap = new HashMap<>();
        statsMap.put("foo", "bar");
        when(watcherService.usageStats()).thenReturn(statsMap);

        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        new WatcherFeatureSet(Settings.EMPTY, licenseState, watcherService).usage(future);
        XPackFeatureSet.Usage watcherUsage = future.get();
        BytesStreamOutput out = new BytesStreamOutput();
        watcherUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new WatcherFeatureSet.Usage(out.bytes().streamInput());

        for (XPackFeatureSet.Usage usage : Arrays.asList(watcherUsage, serializedUsage)) {
            XContentBuilder builder = jsonBuilder();
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);

            XContentSource source = new XContentSource(builder);
            assertThat(source.getValue("foo"), is("bar"));

            assertThat(usage, instanceOf(WatcherFeatureSet.Usage.class));
            WatcherFeatureSet.Usage featureSetUsage = (WatcherFeatureSet.Usage) usage;
            assertThat(featureSetUsage.stats(), hasEntry("foo", "bar"));
        }
    }
}
