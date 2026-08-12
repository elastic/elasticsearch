/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.OptionalLong;
import java.util.Set;

public class NativeMemoryCgroupBackstopTests extends ESTestCase {

    /** Creates a backstop with injected cgroup probes — no doStart() needed for unit tests. */
    private static NativeMemoryCgroupBackstop backstop(int watermark, long usage, long limit) {
        Settings settings = Settings.builder().put(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING.getKey(), watermark).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING, NativeMemoryCgroupBackstop.POLL_INTERVAL_SETTING)
        );
        return new NativeMemoryCgroupBackstop(settings, clusterSettings, null /* threadPool unused in check() */) {
            @Override
            OptionalLong readCgroupUsage() {
                return usage < 0 ? OptionalLong.empty() : OptionalLong.of(usage);
            }

            @Override
            OptionalLong readCgroupLimit() {
                return limit < 0 ? OptionalLong.empty() : OptionalLong.of(limit);
            }
        };
    }

    public void testNoCgroupLimitKeepsBackstopInactive() {
        NativeMemoryCgroupBackstop b = backstop(85, -1L /* empty usage */, -1L /* empty limit */);
        b.check();
        assertFalse("should not refuse when cgroup probes are empty", b.isRefusing());
    }

    public void testNoCgroupUsageKeepsBackstopInactive() {
        NativeMemoryCgroupBackstop b = backstop(85, -1L /* empty */, 1_000_000_000L);
        b.check();
        assertFalse("should not refuse when usage probe is empty", b.isRefusing());
    }

    public void testZeroLimitIgnored() {
        NativeMemoryCgroupBackstop b = backstop(85, 900_000_000L, 0L);
        b.check();
        assertFalse("should not refuse when cgroup limit is 0", b.isRefusing());
    }

    public void testBelowWatermarkStaysOpen() {
        long limit = 1_000_000_000L;
        long usage = 840_000_000L; // 84%
        NativeMemoryCgroupBackstop b = backstop(85, usage, limit);
        b.check();
        assertFalse("should not refuse when below watermark", b.isRefusing());
    }

    public void testAtWatermarkStartsRefusing() {
        long limit = 1_000_000_000L;
        long usage = 850_000_000L; // 85%
        NativeMemoryCgroupBackstop b = backstop(85, usage, limit);
        b.check();
        assertTrue("should refuse when usage >= watermark", b.isRefusing());
    }

    public void testAboveWatermarkStartsRefusing() {
        long limit = 1_000_000_000L;
        long usage = 900_000_000L; // 90%
        NativeMemoryCgroupBackstop b = backstop(85, usage, limit);
        b.check();
        assertTrue("should refuse when above watermark", b.isRefusing());
    }

    public void testHysteresisPreventsPrematureResume() {
        long limit = 1_000_000_000L;
        int wm = 85;
        int hysteresis = NativeMemoryCgroupBackstop.HYSTERESIS_PERCENT;
        int resumeThreshold = wm - hysteresis; // 80
        long dropUsage = resumeThreshold * 10_000_000L; // exactly at 80%

        final long[] usageHolder = { wm * 10_000_000L, dropUsage };
        final int[] callCount = { 0 };
        Settings settings = Settings.builder().put(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING.getKey(), wm).build();
        ClusterSettings cs = new ClusterSettings(
            settings,
            Set.of(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING, NativeMemoryCgroupBackstop.POLL_INTERVAL_SETTING)
        );
        NativeMemoryCgroupBackstop backstop = new NativeMemoryCgroupBackstop(settings, cs, null) {
            @Override
            OptionalLong readCgroupUsage() {
                return OptionalLong.of(usageHolder[Math.min(callCount[0], usageHolder.length - 1)]);
            }

            @Override
            OptionalLong readCgroupLimit() {
                return OptionalLong.of(limit);
            }
        };

        backstop.check(); // usage=85% → refusing
        callCount[0]++;
        assertTrue("should be refusing after first check", backstop.isRefusing());

        backstop.check(); // usage=80% exactly at resume threshold — not strictly below, so still refusing
        assertTrue("should remain refusing at exactly the resume threshold (not strictly below)", backstop.isRefusing());
    }

    public void testDropsBelowHysteresisResumes() {
        long limit = 1_000_000_000L;
        int wm = 85;
        int belowResume = wm - NativeMemoryCgroupBackstop.HYSTERESIS_PERCENT - 1; // 79%

        final long[] usageHolder = { wm * 10_000_000L, belowResume * 10_000_000L };
        final int[] callCount = { 0 };
        Settings settings = Settings.builder().put(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING.getKey(), wm).build();
        ClusterSettings cs = new ClusterSettings(
            settings,
            Set.of(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING, NativeMemoryCgroupBackstop.POLL_INTERVAL_SETTING)
        );
        NativeMemoryCgroupBackstop backstop = new NativeMemoryCgroupBackstop(settings, cs, null) {
            @Override
            OptionalLong readCgroupUsage() {
                return OptionalLong.of(usageHolder[Math.min(callCount[0], usageHolder.length - 1)]);
            }

            @Override
            OptionalLong readCgroupLimit() {
                return OptionalLong.of(limit);
            }
        };

        backstop.check(); // push over watermark → refusing
        callCount[0]++;
        assertTrue("should be refusing after first check", backstop.isRefusing());

        backstop.check(); // drop to 79% (below resume threshold 80%) → resume
        assertFalse("should resume when usage drops below the hysteresis threshold", backstop.isRefusing());
    }

    public void testDynamicWatermarkUpdate() {
        long limit = 1_000_000_000L;
        int wm = 85;
        int usage = 86; // above initial watermark

        Settings settings = Settings.builder().put(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING.getKey(), wm).build();
        ClusterSettings cs = new ClusterSettings(
            settings,
            Set.of(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING, NativeMemoryCgroupBackstop.POLL_INTERVAL_SETTING)
        );
        NativeMemoryCgroupBackstop backstop = new NativeMemoryCgroupBackstop(settings, cs, null) {
            @Override
            OptionalLong readCgroupUsage() {
                return OptionalLong.of(usage * 10_000_000L);
            }

            @Override
            OptionalLong readCgroupLimit() {
                return OptionalLong.of(limit);
            }
        };

        backstop.check();
        assertTrue("should refuse at wm=85, usage=86%", backstop.isRefusing());

        // Raise to 90 — usage (86%) is below the new watermark but NOT below wm-hysteresis (85%), so hysteresis keeps it refusing
        cs.applySettings(Settings.builder().put(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING.getKey(), 90).build());
        backstop.check();
        assertTrue("hysteresis should keep refusing until below 85%", backstop.isRefusing());

        // Lower to 80 — usage 86% > 80, stay refusing
        cs.applySettings(Settings.builder().put(NativeMemoryCgroupBackstop.HIGH_WATERMARK_SETTING.getKey(), 80).build());
        backstop.check();
        assertTrue("still refusing at wm=80, usage=86%", backstop.isRefusing());
    }

    public void testRefusingStartsFalse() {
        NativeMemoryCgroupBackstop b = backstop(85, 0L, 1_000_000_000L);
        assertFalse("should start non-refusing", b.isRefusing());
    }
}
