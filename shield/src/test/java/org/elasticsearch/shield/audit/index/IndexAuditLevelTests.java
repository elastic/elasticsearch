/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.EnumSet;
import java.util.Locale;

import static org.hamcrest.Matchers.*;

public class IndexAuditLevelTests extends ESTestCase {

    @Test
    public void testAllIndexAuditLevel() {
        EnumSet<IndexAuditLevel> enumSet = IndexAuditLevel.parse(new String[] { "_all" });
        IndexAuditLevel[] levels = IndexAuditLevel.values();
        assertThat(enumSet.size(), is(levels.length));
        for (IndexAuditLevel level : levels) {
            assertThat(enumSet.contains(level), is(true));
        }
    }

    @Test
    public void testExcludeHasPreference() {
        EnumSet<IndexAuditLevel> enumSet = IndexAuditLevel.parse(new String[] { "_all" }, new String[] { "_all" });
        assertThat(enumSet.size(), is(0));
    }

    @Test
    public void testExcludeHasPreferenceSingle() {
        String excluded = randomFrom(IndexAuditLevel.values()).toString().toLowerCase(Locale.ROOT);
        EnumSet<IndexAuditLevel> enumSet = IndexAuditLevel.parse(new String[] { "_all" }, new String[] { excluded });
        EnumSet<IndexAuditLevel> expected = EnumSet.allOf(IndexAuditLevel.class);
        expected.remove(IndexAuditLevel.valueOf(excluded.toUpperCase(Locale.ROOT)));
        assertThat(enumSet, equalTo(expected));
    }

}
