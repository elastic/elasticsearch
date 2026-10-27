/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

public class SettingsReadRecorderTests extends ESTestCase {

    public void testTypedReadsAreRecorded() {
        Settings source = Settings.builder()
            .put("mode", "search")
            .put("ignore_case", "true")
            .put("minimum_length", 5)
            .putList("stopwords", "a", "b")
            .build();
        SettingsReadRecorder.Recorded recorded = SettingsReadRecorder.record(source);
        Settings settings = recorded.settings();

        assertEquals("search", settings.get("mode"));
        assertEquals(Boolean.TRUE, settings.getAsBoolean("ignore_case", false));
        assertEquals(Integer.valueOf(5), settings.getAsInt("minimum_length", -1));
        assertEquals(List.of("a", "b"), settings.getAsList("stopwords"));
        assertNull(settings.get("absent"));
        assertEquals("fallback", settings.get("absent_with_default", "fallback"));

        assertEquals(Set.of("mode", "ignore_case", "minimum_length", "stopwords", "absent", "absent_with_default"), recorded.reads());
    }

    public void testIterationIsMarked() {
        SettingsReadRecorder.Recorded recorded = SettingsReadRecorder.record(Settings.builder().put("a", "1").build());
        assertEquals(Set.of("a"), recorded.settings().keySet());
        assertTrue(recorded.reads().contains(SettingsReadRecorder.ITERATED));
    }

    public void testDerivedSettingsAreMarkedIterated() {
        SettingsReadRecorder.Recorded recorded = SettingsReadRecorder.record(Settings.builder().put("group.a", "1").build());
        assertEquals("1", recorded.settings().getByPrefix("group.").get("a"));
        assertTrue(
            "getByPrefix copies the map and drops the recorder; it must be marked",
            recorded.reads().contains(SettingsReadRecorder.ITERATED)
        );
    }

    public void testEmptySourceIsRecordable() {
        SettingsReadRecorder.Recorded recorded = SettingsReadRecorder.record(Settings.EMPTY);
        assertNull(recorded.settings().get("anything"));
        assertEquals(Set.of("anything"), recorded.reads());
        // the EMPTY singleton itself must stay untouched
        assertNull(Settings.EMPTY.get("anything"));
    }
}
