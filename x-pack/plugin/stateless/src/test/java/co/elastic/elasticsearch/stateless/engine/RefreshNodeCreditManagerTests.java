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

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;

public class RefreshNodeCreditManagerTests extends ESTestCase {

    public void testCreditConsumption() {
        long startTimeMillis = randomNonNegativeLong();
        AtomicLong timeMillis = new AtomicLong(startTimeMillis);
        RefreshNodeCreditManager nodeCreditManager = new RefreshNodeCreditManager(timeMillis::get, 1.0, "unknown");
        assertThat(nodeCreditManager.getCredit(), equalTo(720.0));
        for (int i = 0; i < 720; i++) {
            assertTrue(nodeCreditManager.consumeCredit());
        }
        assertFalse(nodeCreditManager.consumeCredit());
    }

    public void testUpdateCredits() {
        long startTimeMillis = randomNonNegativeLong();
        AtomicLong timeMillis = new AtomicLong(startTimeMillis);
        RefreshNodeCreditManager nodeCreditManager = new RefreshNodeCreditManager(timeMillis::get, 1.5, "unknown");
        assertThat(nodeCreditManager.getCredit(), equalTo(1080.0));
        for (int i = 0; i < 1080; i++) {
            assertTrue(nodeCreditManager.consumeCredit());
        }
        assertFalse(nodeCreditManager.consumeCredit());
        assertFalse(nodeCreditManager.consumeCredit());
        assertFalse(nodeCreditManager.consumeCredit());
        assertThat(nodeCreditManager.getCredit(), equalTo(-3.0));
        timeMillis.set(startTimeMillis + 6_000); // add double credits since credits are negative. 2 * 6000 * 1.5 / 5000 = 3.6 capped at 0.
        assertFalse(nodeCreditManager.consumeCredit());
        assertThat(nodeCreditManager.getCredit(), equalTo(-1.0));
        timeMillis.set(startTimeMillis + 6_000 + 100_300); // Adds 100300 * 1.5 / 5000 = 30.09 credits
        assertTrue(nodeCreditManager.consumeCredit());
        assertThat(nodeCreditManager.getCredit(), equalTo(28.09));
    }

    public void testMaxAndMinCredits() {
        long startTimeMillis = randomNonNegativeLong();
        AtomicLong timeMillis = new AtomicLong(startTimeMillis);
        RefreshNodeCreditManager nodeCreditManager = new RefreshNodeCreditManager(timeMillis::get, 2.0, "unknown");
        assertTrue(nodeCreditManager.consumeCredit());
        assertThat(nodeCreditManager.getCredit(), equalTo(1439.0));
        timeMillis.set(startTimeMillis + randomLongBetween(100_000, 105_000)); // Would add many credits, but are maxed at 1440
        assertTrue(nodeCreditManager.consumeCredit());
        assertThat(nodeCreditManager.getCredit(), equalTo(1439.0));
        for (int i = 0; i < 1439; i++) {
            assertTrue(nodeCreditManager.consumeCredit());
        }
        assertThat(nodeCreditManager.getCredit(), equalTo(0.0));
        for (int i = 0; i < 1450; i++) {
            assertFalse(nodeCreditManager.consumeCredit());
        }
        assertThat(nodeCreditManager.getCredit(), equalTo(-1440.0)); // Min is -1440
    }

    public void testUpdateMultiplier() {
        long startTimeMillis = randomNonNegativeLong();
        AtomicLong timeMillis = new AtomicLong(startTimeMillis);
        RefreshNodeCreditManager nodeCreditManager = new RefreshNodeCreditManager(timeMillis::get, 2.0, "unknown");
        assertThat(nodeCreditManager.getCredit(), equalTo(1440.0));
        timeMillis.set(startTimeMillis + 5_000); // Would add 2 credits, but are maxed at 1440
        assertTrue(nodeCreditManager.consumeCredit());
        assertThat(nodeCreditManager.getCredit(), equalTo(1439.0));
        nodeCreditManager.setMultiplier(3);
        long msPassed = randomLongBetween(10_000, 15_000) - 5_000;
        double creditsAccumulated = msPassed * 3.0 / 5000;
        timeMillis.set(startTimeMillis + 5_000 + msPassed);
        assertTrue(nodeCreditManager.consumeCredit());
        assertThat(nodeCreditManager.getCredit(), equalTo(1439.0 + creditsAccumulated - 1.0));
        timeMillis.set(startTimeMillis + randomLongBetween(2_000_000, 2_005_000)); // Would add a lot of credits, but maxed at 2160
        assertTrue(nodeCreditManager.consumeCredit());
        assertThat(nodeCreditManager.getCredit(), equalTo(2159.0));
    }

    @TestLogging(
        reason = "testing warning of node level refresh credit manager",
        value = "co.elastic.elasticsearch.stateless.engine.RefreshNodeCreditManager:WARN"
    )
    public void testPeriodicWarning() {
        long startTimeMillis = randomNonNegativeLong();
        AtomicLong timeMillis = new AtomicLong(startTimeMillis);
        RefreshNodeCreditManager nodeCreditManager = new RefreshNodeCreditManager(timeMillis::get, 1.0, "unknown");
        assertThat(nodeCreditManager.getCredit(), equalTo(720.0));
        for (int i = 0; i < 720; i++) {
            assertTrue(nodeCreditManager.consumeCredit());
        }

        Function<String, Boolean> consumeCreditAndCheckWarningMessage = (shouldSeeWarning) -> {
            final MockLogAppender mockLogAppender = new MockLogAppender();
            try (var ignored = mockLogAppender.capturing(RefreshNodeCreditManager.class)) {
                MockLogAppender.EventuallySeenEventExpectation expectation = new MockLogAppender.EventuallySeenEventExpectation(
                    "node refresh throttling warning",
                    RefreshNodeCreditManager.class.getCanonicalName(),
                    Level.WARN,
                    "The refresh throttler for [unknown] indices has been "
                        + shouldSeeWarning
                        + ". External refreshes on this "
                        + "node are throttled to a maximum [1.0] refreshes per [5] seconds on average every [60] minutes. Decrease "
                        + "the number of refreshes to suppress this message. This message will be suppressed for the next [5] minutes."
                );
                if (shouldSeeWarning.isEmpty() == false) expectation.setExpectSeen();
                mockLogAppender.addExpectation(expectation);
                Boolean result = nodeCreditManager.consumeCredit();
                mockLogAppender.assertAllExpectationsMatched();
                return result;
            }
        };

        assertFalse(consumeCreditAndCheckWarningMessage.apply("triggered")); // throttled and warning is seen
        timeMillis.set(startTimeMillis + randomLongBetween(10_000, 15_000)); // add two credits. one full credit available now.
        assertTrue(consumeCreditAndCheckWarningMessage.apply("")); // not throttled and warning is not seen
        assertFalse(consumeCreditAndCheckWarningMessage.apply("")); // throttled but warning is not seen (last is within 5 mins)

        timeMillis.set(startTimeMillis + 4_000_000); // more than an hour away to regain max credits
        // Consume all credits to minimum
        for (int i = 0; i < 720 * 2; i++) {
            nodeCreditManager.consumeCredit();
        }
        assertThat(nodeCreditManager.getCredit(), equalTo(-720.0));
        timeMillis.set(startTimeMillis + 4_299_000); // almost 5 minutes passed, giving less than 120 credits (since credits are doubled)
        // Reach minimum again
        for (int i = 0; i < 120; i++) {
            nodeCreditManager.consumeCredit();
        }
        assertThat(nodeCreditManager.getCredit(), equalTo(-720.0));
        timeMillis.set(startTimeMillis + 4_300_000); // 5 minutes passed since last warning
        assertFalse(consumeCreditAndCheckWarningMessage.apply("exhausted")); // throttled and exhausted warning message is seen
    }
}
