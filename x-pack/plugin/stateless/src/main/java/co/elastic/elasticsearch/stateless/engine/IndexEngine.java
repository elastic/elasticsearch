/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.RefreshResult;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * {@link Engine} implementation for index shards
 */
public class IndexEngine extends InternalEngine {

    public static final Setting<TimeValue> INDEX_FLUSH_INTERVAL_SETTING = Setting.timeSetting(
        "index.translog.flush_interval",
        new TimeValue(5, TimeUnit.SECONDS),
        new TimeValue(-1, TimeUnit.MILLISECONDS),
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    private final LongSupplier relativeTimeInNanosSupplier;

    private final AtomicLong lastFlushNanos;
    private volatile TimeValue indexFlushInterval;

    public IndexEngine(EngineConfig engineConfig) {
        super(engineConfig);
        this.relativeTimeInNanosSupplier = config().getRelativeTimeInNanosSupplier();
        this.lastFlushNanos = new AtomicLong(relativeTimeInNanosSupplier.getAsLong());
        this.indexFlushInterval = INDEX_FLUSH_INTERVAL_SETTING.get(config().getIndexSettings().getSettings());
    }

    @Override
    public void onSettingsChanged() {
        super.onSettingsChanged();
        this.indexFlushInterval = INDEX_FLUSH_INTERVAL_SETTING.get(config().getIndexSettings().getSettings());
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        final TimeValue flushInterval = indexFlushInterval;
        boolean shouldFlushBecauseInterval = flushInterval.duration() != -1
            && (relativeTimeInNanosSupplier.getAsLong() - lastFlushNanos.get()) >= flushInterval.nanos();
        // TODO flush only if `indexWriter.hasUncommittedChanges() == true`
        return shouldFlushBecauseInterval || super.shouldPeriodicallyFlush();
    }

    @Override
    public boolean flush(boolean force, boolean waitIfOngoing) throws EngineException {
        boolean result = super.flush(force, waitIfOngoing);
        if (result) {
            lastFlushNanos.set(relativeTimeInNanosSupplier.getAsLong());
        }
        return result;
    }

    // visible for testing
    long getLastFlushNanos() {
        return lastFlushNanos.get();
    }

    @Override
    public RefreshResult refresh(String source) throws EngineException {
        if (source.equals(TransportShardRefreshAction.SOURCE_API)) {
            flush(true, true);
        }
        return super.refresh(source);
    }
}
