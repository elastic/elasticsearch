/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

public class FsService {

    private static final Logger logger = LogManager.getLogger(FsService.class);

    private final Supplier<FsInfo> fsInfoSupplier;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.fs.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    // permits tests to bypass the refresh interval on the cache; deliberately unregistered since it is only for use in tests
    public static final Setting<Boolean> ALWAYS_REFRESH_SETTING = Setting.boolSetting(
        "monitor.fs.always_refresh",
        false,
        Property.NodeScope
    );

    public FsService(final Settings settings, final NodeEnvironment nodeEnvironment) {
        final FsProbe probe = new FsProbe(nodeEnvironment);
        final FsInfo initialValue = stats(probe, null);
        if (ALWAYS_REFRESH_SETTING.get(settings)) {
            assert REFRESH_INTERVAL_SETTING.exists(settings) == false;
            logger.debug("bypassing refresh_interval");
            fsInfoSupplier = () -> stats(probe, initialValue);
        } else {
            final TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
            logger.debug("using refresh_interval [{}]", refreshInterval);
            final FsInfoCache fsInfoCache = new FsInfoCache(refreshInterval, initialValue, probe);
            fsInfoSupplier = () -> {
                try {
                    return fsInfoCache.getOrRefresh();
                } catch (UncheckedIOException e) {
                    logger.debug("unexpected exception reading filesystem info", e);
                    return null;
                }
            };
        }
    }

    public FsInfo stats() {
        return fsInfoSupplier.get();
    }

    private static FsInfo stats(FsProbe probe, FsInfo initialValue) {
        try {
            return probe.stats(initialValue);
        } catch (IOException e) {
            logger.debug("unexpected exception reading filesystem info", e);
            return null;
        }
    }

    private static class FsInfoCache extends SingleObjectCache<FsInfo> {

        private final FsInfo initialValue;
        private final FsProbe probe;

        FsInfoCache(TimeValue interval, FsInfo initialValue, FsProbe probe) {
            super(interval, initialValue);
            this.initialValue = initialValue;
            this.probe = probe;
        }

        @Override
        protected FsInfo refresh() {
            try {
                return probe.stats(initialValue);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

    }

}
