/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OldObjectsSampler {
    private static final int NUM_SAMPLES = 3;
    private static final int SAMPLER_DELAY = 3_000;
    private static final Logger logger = LogManager.getLogger(OldObjectsSampler.class);

    private static List<RecordedEvent> fromRecording(Recording recording) throws IOException {
        return RecordingFile.readAllEvents(dump(recording));
    }

    private static Path dump(Recording recording) throws IOException {
        Path p = recording.getDestination();
        if (p == null) {
            File directory = new File(".");
            ProcessHandle h = ProcessHandle.current();
            p = new File(directory.getAbsolutePath(), "recording-" + recording.getId() + "-pid" + h.pid() + ".jfr").toPath();
            recording.dump(p);
        }
        return p;
    }

    private static Map<RecordedEvent, Long> coalesce(List<RecordedEvent> events) {
        Map<RecordedStackTrace, RecordedEvent> stackToEvent = events.stream().collect(
            Collectors.toMap(RecordedEvent::getStackTrace, e -> e, (e1, e2) -> e1));
        Map<RecordedStackTrace, Long> coalescedStacks = events.stream().collect(
            Collectors.groupingBy(RecordedEvent::getStackTrace, Collectors.counting()));

        return coalescedStacks.entrySet().stream().collect(
            Collectors.toMap(e -> stackToEvent.get(e.getKey()), e -> e.getValue(), (e1, e2) -> e1));
    }

    public static void dumpMemoryHogSuspects() throws IOException {
        for (int counter = 0; counter < NUM_SAMPLES; counter++) {
            try (Recording r = new Recording()) {
                r.enable("jdk.OldObjectSample").withStackTrace().with("cutoff", "infinity");
                r.start();
                try {
                    Thread.sleep(SAMPLER_DELAY);
                } catch (Exception ignore) {}
                r.stop();
                List<RecordedEvent> events = fromRecording(r);
                logger.warn("Sampling result {}/{}", counter+1, NUM_SAMPLES);
                if (events.isEmpty() == false) {
                    Map<RecordedEvent, Long> coalesced = coalesce(events);
                    coalesced.entrySet().forEach(e -> {
                        logger.warn("{} instances of:", e.getValue());
                        logger.warn(e.getKey());
                    });
                } else {
                    logger.warn("[No long lived objects found]");
                }
            }
        }
    }
}
