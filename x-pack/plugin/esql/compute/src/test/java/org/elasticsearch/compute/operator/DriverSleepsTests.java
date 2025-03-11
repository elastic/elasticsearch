/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.core.IsEqual.equalTo;

public class DriverSleepsTests extends AbstractWireSerializingTestCase<DriverSleeps> {
    public static DriverSleeps randomDriverSleeps() {
        return randomDriverSleeps(between(0, DriverSleeps.RECORDS * 3));
    }

    private static DriverSleeps randomDriverSleeps(int cycles) {
        DriverSleeps sleeps = DriverSleeps.empty();
        long now = 0;
        for (int i = 0; i < cycles; i++) {
            now += between(1, 100000);
            sleeps = sleeps.sleep(randomSleepReason(), now);
            if (i != cycles - 1 || randomBoolean()) {
                // Randomly don't wake on the last sleep
                now += between(1, 100000);
                sleeps = sleeps.wake(now);
            }
        }
        return sleeps;
    }

    private static String randomSleepReason() {
        return randomFrom("driver time", "driver iteration", "exchange empty", "exchange full");
    }

    public void testEmptyToXContent() {
        assertThat(Strings.toString(DriverSleeps.empty(), true, true), equalTo("""
            {
              "counts" : { },
              "first" : [ ],
              "last" : [ ]
            }"""));
    }

    public void testSleepingToXContent() {
        assertThat(Strings.toString(DriverSleeps.empty().sleep("driver iterations", 1723555763000L), true, true), equalTo("""
            {
              "counts" : {
                "driver iterations" : 1
              },
              "first" : [
                {
                  "reason" : "driver iterations",
                  "sleep" : "2024-08-13T13:29:23.000Z",
                  "sleep_millis" : 1723555763000
                }
              ],
              "last" : [
                {
                  "reason" : "driver iterations",
                  "sleep" : "2024-08-13T13:29:23.000Z",
                  "sleep_millis" : 1723555763000
                }
              ]
            }"""));
    }

    public void testWakingToXContent() {
        assertThat(
            Strings.toString(DriverSleeps.empty().sleep("driver iterations", 1723555763000L).wake(1723555863000L), true, true),
            equalTo("""
                {
                  "counts" : {
                    "driver iterations" : 1
                  },
                  "first" : [
                    {
                      "reason" : "driver iterations",
                      "sleep" : "2024-08-13T13:29:23.000Z",
                      "sleep_millis" : 1723555763000,
                      "wake" : "2024-08-13T13:31:03.000Z",
                      "wake_millis" : 1723555863000
                    }
                  ],
                  "last" : [
                    {
                      "reason" : "driver iterations",
                      "sleep" : "2024-08-13T13:29:23.000Z",
                      "sleep_millis" : 1723555763000,
                      "wake" : "2024-08-13T13:31:03.000Z",
                      "wake_millis" : 1723555863000
                    }
                  ]
                }""")
        );
    }

    @Override
    protected Writeable.Reader<DriverSleeps> instanceReader() {
        return DriverSleeps::read;
    }

    @Override
    protected DriverSleeps createTestInstance() {
        return randomDriverSleeps();
    }

    @Override
    protected DriverSleeps mutateInstance(DriverSleeps instance) throws IOException {
        if (instance.last().isEmpty()) {
            return instance.sleep(randomSleepReason(), between(1, 10000));
        }
        DriverSleeps.Sleep last = instance.last().get(instance.last().size() - 1);
        if (last.isStillSleeping()) {
            return instance.wake(last.sleep() + between(1, 10000));
        }
        return instance.sleep(randomSleepReason(), last.wake() + between(1, 10000));
    }

    public void testTracking() throws IOException {
        long now = 0;
        DriverSleeps sleeps = DriverSleeps.empty();

        Map<String, Long> expectedCounts = new TreeMap<>();
        List<DriverSleeps.Sleep> expectedFirst = new ArrayList<>();
        assertThat(sleeps, equalTo(new DriverSleeps(expectedCounts, expectedFirst, expectedFirst)));

        /*
         * Simulate sleeping and waking when the records aren't full.
         * New sleeps and wakes should show up in both the "first" and "last" fields.
         */
        for (int i = 0; i < DriverSleeps.RECORDS; i++) {
            now++;
            String reason = randomSleepReason();
            expectedCounts.compute(reason, (k, v) -> v == null ? 1 : v + 1);

            sleeps = sleeps.sleep(reason, now);
            expectedFirst.add(new DriverSleeps.Sleep(reason, now, 0));
            assertThat(sleeps, equalTo(new DriverSleeps(expectedCounts, expectedFirst, expectedFirst)));
            assertXContent(sleeps, expectedCounts, expectedFirst, expectedFirst);

            now++;
            sleeps = sleeps.wake(now);
            expectedFirst.set(expectedFirst.size() - 1, new DriverSleeps.Sleep(reason, now - 1, now));
            assertThat(sleeps, equalTo(new DriverSleeps(expectedCounts, expectedFirst, expectedFirst)));
            assertXContent(sleeps, expectedCounts, expectedFirst, expectedFirst);
        }

        /*
         * Simulate sleeping and waking when the records are full.
         * New sleeps and wakes should show up in only the "last" field.
         */
        List<DriverSleeps.Sleep> expectedLast = new ArrayList<>(expectedFirst);
        for (int i = 0; i < 1000; i++) {
            now++;
            String reason = randomSleepReason();
            expectedCounts.compute(reason, (k, v) -> v == null ? 1 : v + 1);

            sleeps = sleeps.sleep(reason, now);
            expectedLast.remove(0);
            expectedLast.add(new DriverSleeps.Sleep(reason, now, 0));
            assertThat(sleeps, equalTo(new DriverSleeps(expectedCounts, expectedFirst, expectedLast)));
            assertXContent(sleeps, expectedCounts, expectedFirst, expectedLast);

            now++;
            sleeps = sleeps.wake(now);
            expectedLast.set(expectedLast.size() - 1, new DriverSleeps.Sleep(reason, now - 1, now));
            assertThat(sleeps, equalTo(new DriverSleeps(expectedCounts, expectedFirst, expectedLast)));
            assertXContent(sleeps, expectedCounts, expectedFirst, expectedLast);
        }
    }

    public void assertXContent(
        DriverSleeps sleeps,
        Map<String, Long> expectedCounts,
        List<DriverSleeps.Sleep> expectedFirst,
        List<DriverSleeps.Sleep> expectedLast
    ) throws IOException {
        try (BytesStreamOutput expected = new BytesStreamOutput()) {
            try (XContentBuilder b = new XContentBuilder(XContentType.JSON.xContent(), expected).prettyPrint().humanReadable(true)) {
                b.startObject();
                b.startObject("counts");
                {
                    for (Map.Entry<String, Long> e : expectedCounts.entrySet()) {
                        b.field(e.getKey(), e.getValue());
                    }
                }
                b.endObject();
                {
                    b.startArray("first");
                    for (DriverSleeps.Sleep sleep : expectedFirst) {
                        sleep.toXContent(b, ToXContent.EMPTY_PARAMS);
                    }
                    b.endArray();
                }
                {
                    b.startArray("last");
                    for (DriverSleeps.Sleep sleep : expectedLast) {
                        sleep.toXContent(b, ToXContent.EMPTY_PARAMS);
                    }
                    b.endArray();
                }
                b.endObject();
            }
            assertThat(Strings.toString(sleeps, true, true), equalTo(expected.bytes().utf8ToString()));
        }
    }

    public void testWakeNeverSlept() {
        Exception e = expectThrows(IllegalStateException.class, () -> DriverSleeps.empty().wake(1));
        assertThat(e.getMessage(), equalTo("Never slept."));
    }

    public void testWakeWhileAwake() {
        Exception e = expectThrows(IllegalStateException.class, () -> DriverSleeps.empty().sleep(randomSleepReason(), 1).wake(2).wake(3));
        assertThat(e.getMessage(), equalTo("Already awake."));
    }

    public void testSleepWhileSleeping() {
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> DriverSleeps.empty().sleep(randomSleepReason(), 1).sleep(randomSleepReason(), 2)
        );
        assertThat(e.getMessage(), equalTo("Still sleeping."));
    }
}
