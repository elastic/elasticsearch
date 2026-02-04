/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.Locale;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(filters = { JfrThreadsFilter.class })
public class APMMeterServiceTests extends ESTestCase {

    private APMMeterService meterService;

    public void testDisabledTelemetry() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false).build();

        meterService = new APMMeterService(settings);

        assertThat(meterService.enabled, is(false));
        assertThat(meterService.getMeterRegistry().getLongGauge("system.memory.total"), is(nullValue()));
    }

    public void testEnabledWithApmAgent() {
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true)
            .put(APMAgentSettings.TELEMETRY_OTEL_METRICS_ENABLED_SETTING.getKey(), false)
            .build();

        meterService = new APMMeterService(settings);

        assertThat(meterService.enabled, is(true));
        // APM Agent provides its own JVM metrics, so SystemMetrics not registered
        assertThat(meterService.getMeterRegistry().getLongGauge("system.memory.total"), is(nullValue()));
    }

    public void testEnabledWithOtelSdk() {
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true)
            .put(APMAgentSettings.TELEMETRY_OTEL_METRICS_ENABLED_SETTING.getKey(), true)
            .put("telemetry.agent.server_url", "http://localhost:8200")
            .build();

        meterService = new APMMeterService(settings);

        assertThat(meterService.enabled, is(true));
        assertThat(meterService.getMeterRegistry().getLongGauge("system.memory.total"), notNullValue());
        assertThat(meterService.getMeterRegistry().getLongGauge("system.memory.actual.free"), notNullValue());
        assertThat(meterService.getMeterRegistry().getLongGauge("system.process.memory.size"), notNullValue());
        if (isLinux()) {
            assertThat(meterService.getMeterRegistry().getLongGauge("system.process.cgroup.memory.mem.limit.bytes"), notNullValue());
            assertThat(meterService.getMeterRegistry().getLongGauge("system.process.cgroup.memory.mem.usage.bytes"), notNullValue());
        } else {
            assertThat(meterService.getMeterRegistry().getLongGauge("system.process.cgroup.memory.mem.limit.bytes"), is(nullValue()));
            assertThat(meterService.getMeterRegistry().getLongGauge("system.process.cgroup.memory.mem.usage.bytes"), is(nullValue()));
        }
    }

    private static boolean isLinux() {
        String osName = System.getProperty("os.name");
        return osName != null && osName.toLowerCase(Locale.ROOT).contains("linux");
    }

    @After
    public void tearDown() throws Exception {
        if (meterService != null) {
            meterService.start();
            meterService.close();
        }
        super.tearDown();
    }
}
