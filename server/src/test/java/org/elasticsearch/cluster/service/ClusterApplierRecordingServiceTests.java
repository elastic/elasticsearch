/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Recorder;
import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Stats.Recording;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.contains;

public class ClusterApplierRecordingServiceTests extends ESTestCase {

    public void testRecorder() {
        long[] currentTime = new long[1];
        var recorder = new Recorder(() -> currentTime[0]);
        {
            Releasable releasable = recorder.record("action1");
            currentTime[0] = 5;
            releasable.close();
        }
        {
            Releasable releasable = recorder.record("action2");
            currentTime[0] = 42;
            releasable.close();
        }
        {
            Releasable releasable = recorder.record("action3");
            currentTime[0] = 45;
            releasable.close();
        }

        var recordings = recorder.getRecordings();
        assertThat(recordings, contains(Tuple.tuple("action1", 5L), Tuple.tuple("action2", 37L), Tuple.tuple("action3", 3L)));
    }

    public void testRecorderAlreadyRecording() {
        var recorder = new Recorder(() -> 1L);
        Releasable releasable = recorder.record("action1");
        expectThrows(IllegalStateException.class, () -> recorder.record("action2"));
    }

    public void testRecordingServiceStats() {
        var service = new ClusterApplierRecordingService();

        {
            long[] currentTime = new long[1];
            var recorder = new Recorder(() -> currentTime[0]);
            try (var r = recorder.record("action1")) {
                currentTime[0] = 5;
            }
            try (var r = recorder.record("action2")) {
                currentTime[0] = 42;
            }
            try (var r = recorder.record("action3")) {
                currentTime[0] = 45;
            }
            service.updateStats(recorder);
            var stats = service.getStats();
            assertThat(
                stats.getRecordings().entrySet(),
                contains(
                    Map.entry("action2", new Recording(1, 37)),
                    Map.entry("action1", new Recording(1, 5)),
                    Map.entry("action3", new Recording(1, 3))
                )
            );
        }
        {
            long[] currentTime = new long[1];
            var recorder = new Recorder(() -> currentTime[0]);
            try (var r = recorder.record("action1")) {
                currentTime[0] = 3;
            }
            try (var r = recorder.record("action2")) {
                currentTime[0] = 35;
            }
            try (var r = recorder.record("action3")) {
                currentTime[0] = 41;
            }
            service.updateStats(recorder);
            var stats = service.getStats();
            assertThat(
                stats.getRecordings().entrySet(),
                contains(
                    Map.entry("action2", new Recording(2, 69)),
                    Map.entry("action3", new Recording(2, 9)),
                    Map.entry("action1", new Recording(2, 8))
                )
            );
        }
        {
            long[] currentTime = new long[1];
            var recorder = new Recorder(() -> currentTime[0]);
            try (var r = recorder.record("action1")) {
                currentTime[0] = 2;
            }
            try (var r = recorder.record("action3")) {
                currentTime[0] = 6;
            }
            service.updateStats(recorder);
            var stats = service.getStats();
            assertThat(
                stats.getRecordings().entrySet(),
                contains(Map.entry("action3", new Recording(3, 13)), Map.entry("action1", new Recording(3, 10)))
            );
        }
    }

}
