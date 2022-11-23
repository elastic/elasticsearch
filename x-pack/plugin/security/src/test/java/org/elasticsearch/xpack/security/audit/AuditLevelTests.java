/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class AuditLevelTests extends ESTestCase {
    public void testAllIndexAuditLevel() {
        EnumSet<AuditLevel> enumSet = AuditLevel.parse(Collections.singletonList("_all"));
        AuditLevel[] levels = AuditLevel.values();
        assertThat(enumSet.size(), is(levels.length));
        for (AuditLevel level : levels) {
            assertThat(enumSet.contains(level), is(true));
        }
    }

    public void testExcludeHasPreference() {
        EnumSet<AuditLevel> enumSet = AuditLevel.parse(Collections.singletonList("_all"), Collections.singletonList("_all"));
        assertThat(enumSet.size(), is(0));
    }

    public void testExcludeHasPreferenceSingle() {
        String excluded = randomFrom(AuditLevel.values()).toString().toLowerCase(Locale.ROOT);
        EnumSet<AuditLevel> enumSet = AuditLevel.parse(Collections.singletonList("_all"), Collections.singletonList(excluded));
        EnumSet<AuditLevel> expected = EnumSet.allOf(AuditLevel.class);
        expected.remove(AuditLevel.valueOf(excluded.toUpperCase(Locale.ROOT)));
        assertThat(enumSet, equalTo(expected));
    }
}
