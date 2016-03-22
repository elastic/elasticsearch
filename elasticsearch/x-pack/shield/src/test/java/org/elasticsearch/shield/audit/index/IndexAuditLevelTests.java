/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexAuditLevelTests extends ESTestCase {
    public void testAllIndexAuditLevel() {
        EnumSet<IndexAuditLevel> enumSet = IndexAuditLevel.parse(Collections.singletonList("_all"));
        IndexAuditLevel[] levels = IndexAuditLevel.values();
        assertThat(enumSet.size(), is(levels.length));
        for (IndexAuditLevel level : levels) {
            assertThat(enumSet.contains(level), is(true));
        }
    }

    public void testExcludeHasPreference() {
        EnumSet<IndexAuditLevel> enumSet = IndexAuditLevel.parse(Collections.singletonList("_all"), Collections.singletonList("_all"));
        assertThat(enumSet.size(), is(0));
    }

    public void testExcludeHasPreferenceSingle() {
        String excluded = randomFrom(IndexAuditLevel.values()).toString().toLowerCase(Locale.ROOT);
        EnumSet<IndexAuditLevel> enumSet = IndexAuditLevel.parse(Collections.singletonList("_all"), Collections.singletonList(excluded));
        EnumSet<IndexAuditLevel> expected = EnumSet.allOf(IndexAuditLevel.class);
        expected.remove(IndexAuditLevel.valueOf(excluded.toUpperCase(Locale.ROOT)));
        assertThat(enumSet, equalTo(expected));
    }
}
