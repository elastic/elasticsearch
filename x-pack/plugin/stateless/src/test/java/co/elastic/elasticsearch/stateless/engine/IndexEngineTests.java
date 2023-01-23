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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;

public class IndexEngineTests extends EngineTestCase {

    public void testShouldPeriodicallyFlush() throws IOException {

        var configuredFlushIntervalNanos = randomLongBetween(1_000L, 5_000_000_000L);
        var settings = Settings.builder()
            .put(IndexEngine.INDEX_FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueNanos(configuredFlushIntervalNanos))
            .build();

        var currentTime = new AtomicLong(0L);

        IOUtils.close(engine);
        var config = copy(engine.config(), settings, currentTime::get);
        try (var engine = new IndexEngine(config)) {
            // skip recovery, otherwise flush is not possible (see `InternalEngine.ensureCanFlush()`)
            engine.skipTranslogRecovery();

            // should not flush immediately after creation until interval is elapsed
            assertThat(engine.shouldPeriodicallyFlush(), equalTo(false));
            // should flush after interval is elapsed even if there are no prior flushes
            currentTime.addAndGet(configuredFlushIntervalNanos);
            assertThat(engine.shouldPeriodicallyFlush(), equalTo(true));

            // flush and record flush time
            assertThat("Flush time is only recorded when flush=true", engine.flush(false, false), equalTo(true));
            assertThat("Flush time is correctly recorded", engine.getLastFlushNanos(), equalTo(currentTime.get()));
            // should not flush until interval is elapsed
            currentTime.addAndGet(configuredFlushIntervalNanos - 1);
            assertThat(engine.shouldPeriodicallyFlush(), equalTo(false));
            // should flush until after interval is elapsed
            currentTime.addAndGet(1);
            assertThat(engine.shouldPeriodicallyFlush(), equalTo(true));
        }
    }

    private EngineConfig copy(EngineConfig config, Settings additionalIndexSettings, LongSupplier relativeTimeInNanosSupplier) {
        return new EngineConfig(
            config.getShardId(),
            config.getThreadPool(),
            new IndexSettings(
                config.getIndexSettings().getIndexMetadata(),
                Settings.builder().put(config.getIndexSettings().getNodeSettings()).put(additionalIndexSettings).build()
            ),
            config.getWarmer(),
            config.getStore(),
            config.getMergePolicy(),
            config.getAnalyzer(),
            config.getSimilarity(),
            config.getCodecService(),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListener(),
            Collections.emptyList(),
            config.getIndexSort(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            config.getSnapshotCommitSupplier(),
            config.getLeafSorter(),
            relativeTimeInNanosSupplier,
            config.getIndexCommitListener(),
            config.isPromotableToPrimary()
        );
    }
}
