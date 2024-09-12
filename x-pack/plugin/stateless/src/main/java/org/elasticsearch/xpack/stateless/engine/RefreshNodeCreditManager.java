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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static co.elastic.elasticsearch.stateless.engine.RefreshThrottlingService.BUDGET_INTERVAL;
import static co.elastic.elasticsearch.stateless.engine.RefreshThrottlingService.THROTTLING_INTERVAL;

/**
 * A credit manager that can be shared by multiple {@link RefreshBurstableThrottler} shard throttlers. Whenever the credits are consumed,
 * the shard throttler is throttled, and a warning is shown. Credits are gathered in a similar manner as the shard throttler, but with a
 * couple of differences:
 * <ul>
 *     <li>Credits can have a multiplier so that more than 1 credit per throttling interval can be accumulated.</li>
 *     <li>The credits are a <code>double</code> and can be accumulated in fraction.</li>
 *     <li>Credits range <code>[-maxCredits, maxCredits]</code> where max credits are the shard's throttler max credits * multiplier.</li>
 * </ul>
 *
 * Note that the aim of the node credits are to be a safety net of imposing a maximum average of 1 refresh / 5 seconds / 4GB over the past
 * hour. Thus if enough shards are bursting and deplete the node's credits, node-wide refresh throttling is imposed (shards do 1 refresh /
 * 5 sec).
 *
 * In contrast to the shard throttler, the node credits can be negative. While the shard throttler does not allow more than one refresh
 * in a throttled situation, unfortunately the same is not true on the node level since shards are guaranteed a scheduled refresh. E.g., if
 * there are 1M shards, we'd have 1M refreshes happening even when throttled at 1refresh/5sec. Imagine if the node credits could not be
 * negative, they'd be 0. And imagine suddenly that all refreshes stop. The shards and the node will start accumulating credits. This would
 * mean new refreshes on a single shard (for example) will be able to go uninterrupted/bursting, while in the previous hour we had
 * 1M refreshes / 5sec node-wide. This is the problem: bursting is immediately allowed node-wide even though the node limit on the last hour
 * is heavily surpassed, and it should be still throttling. For this reason, we said to allow negative credits in the node credits.
 * There will need to be some time passed for the node credits to recover to positive and allow bursting.
 *
 * Also note we use a heuristic of doubling credit accumulation when credits are negative to help when trying to recover from a throttled
 * situation. Because:
 * <ul>
 *     <li>Otherwise, something that happened longer ago than one hour can affect throttling.</li>
 *     <li>The only way to avoid the above point correctly is to capture a sliding window of requests, which would be more complicated
 *     and keep more data.</li>
 * </ul>
 * Example: imagine that a user is in a very throttled situation, and has depleted the node credits over the last hour. They realize and
 * "fix" their workload to have less refreshes (that would be allowed by the node budget). However, because the node credits were previously
 * exhausted at -maxCredits, the new refreshes will still be throttled for a while. It may take, e.g., a couple of hours for the node
 * credits to become positive again. This is punitive for those users that try to fix their workloads. For this reason, we thought of
 * this "easy" heuristic to be more forgiving is to double the credits accumulation when recovering from negative credits.
 */
public class RefreshNodeCreditManager {

    public static final TimeValue WARNING_CHECK_INTERVAL = TimeValue.timeValueMinutes(5);
    private static final Logger logger = LogManager.getLogger(RefreshNodeCreditManager.class);

    protected final LongSupplier relativeTimeSupplier;
    private final String indicesType;
    private volatile double maxCredit;
    private volatile double multiplier;
    private double credit;
    private final AtomicLong lastWarningMillis = new AtomicLong(-1);
    private long lastCreditUpdate;

    @SuppressWarnings("this-escape")
    public RefreshNodeCreditManager(LongSupplier relativeTimeSupplierInMillis, double multiplier, String indicesType) {
        this.relativeTimeSupplier = relativeTimeSupplierInMillis;
        this.lastCreditUpdate = relativeTimeSupplierInMillis.getAsLong();
        setMultiplier(multiplier);
        this.credit = maxCredit;
        this.indicesType = indicesType;
    }

    public synchronized void setMultiplier(double multiplier) {
        this.multiplier = multiplier;
        this.maxCredit = (BUDGET_INTERVAL.seconds() / THROTTLING_INTERVAL.seconds()) * multiplier;
    }

    /**
     * Consumes a credit and returns true if there was a credit available and thus the refresh should not be throttled. Else it returns
     * false.
     */
    public boolean consumeCredit() {
        double remainingCredit = updateAndConsumeCredit();

        if (remainingCredit >= 0.0) {
            return true;
        } else {
            long relativeTimeMillis = relativeTimeSupplier.getAsLong();
            long lastWarningMillis = this.lastWarningMillis.get();
            if (relativeTimeMillis - lastWarningMillis >= WARNING_CHECK_INTERVAL.getMillis()
                && this.lastWarningMillis.compareAndSet(lastWarningMillis, relativeTimeMillis)) {
                logger.warn(
                    "The refresh throttler for [{}] indices has been {}. External refreshes on this node are throttled to a "
                        + "maximum [{}] refreshes per [{}] seconds on average every [{}] minutes. Decrease the number of refreshes to "
                        + "suppress this message. This message will be suppressed for the next [{}] minutes.",
                    indicesType,
                    remainingCredit == -maxCredit ? "exhausted" : "triggered",
                    multiplier,
                    THROTTLING_INTERVAL.seconds(),
                    BUDGET_INTERVAL.minutes(),
                    WARNING_CHECK_INTERVAL.minutes()
                );
            }
        }
        return false;
    }

    private synchronized double updateAndConsumeCredit() {
        long relativeTimeMillis = relativeTimeSupplier.getAsLong();

        // Update credit
        if (relativeTimeMillis > lastCreditUpdate) {
            long passedMillis = relativeTimeMillis - lastCreditUpdate;
            lastCreditUpdate = relativeTimeMillis;
            double accumulatedCredit = passedMillis * multiplier / THROTTLING_INTERVAL.millis();
            if (credit >= 0.0) {
                credit += accumulatedCredit;
            } else {
                double creditWithStandardAccumulation = credit + accumulatedCredit; // see class description for reasoning on doubling
                double creditWithDoubleAccumulation = Math.min(credit + accumulatedCredit * 2, 0.0); // cap double accumulation to 0
                credit = Math.max(creditWithDoubleAccumulation, creditWithStandardAccumulation);
            }
            credit = Math.min(maxCredit, credit); // cap
            assert credit <= maxCredit;
        }

        // Consume credit, and allow negative number up to -maxCredit
        credit = Math.max(credit - 1.0, -maxCredit);
        return credit;
    }

    public LongSupplier getRelativeTimeSupplier() {
        return relativeTimeSupplier;
    }

    // package private for testing
    double getMultiplier() {
        return multiplier;
    }

    // package private for testing
    double getCredit() {
        return credit;
    }

    // package private for testing
    void setCredit(double credit) {
        this.credit = credit;
    }
}
