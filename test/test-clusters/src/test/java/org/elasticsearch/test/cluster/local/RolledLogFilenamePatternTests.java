/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.LogType;
import org.junit.Test;

import java.util.EnumMap;
import java.util.Map;
import java.util.regex.Matcher;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RolledLogFilenamePatternTests {

    private static final String CLUSTER = "test-cluster";

    /**
     * One rolled file name per log type, built from the {@code filePattern} of its appender in the distribution's and
     * x-pack core's {@code log4j2.properties}.
     */
    private static final Map<LogType, String> ROLLED_EXAMPLES = new EnumMap<>(LogType.class);
    static {
        ROLLED_EXAMPLES.put(LogType.SERVER, "test-cluster-2026-10-18-1.log.gz");
        ROLLED_EXAMPLES.put(LogType.SERVER_JSON, "test-cluster-2026-10-18-1.json.gz");
        ROLLED_EXAMPLES.put(LogType.AUDIT, "test-cluster_audit-2026-10-18-1.json.gz");
        ROLLED_EXAMPLES.put(LogType.SEARCH_SLOW, "test-cluster_index_search_slowlog-1.json.gz");
        ROLLED_EXAMPLES.put(LogType.INDEXING_SLOW, "test-cluster_index_indexing_slowlog-1.json.gz");
        ROLLED_EXAMPLES.put(LogType.ESQL_QUERY, "test-cluster_esql_querylog-1.json.gz");
        ROLLED_EXAMPLES.put(LogType.DEPRECATION, "test-cluster_deprecation-1.json.gz");
    }

    @Test
    public void testEveryLogTypeHasRolledExample() {
        assertThat(ROLLED_EXAMPLES.size(), is(LogType.values().length));
    }

    @Test
    public void testRolledFilenameMatchesOnlyItsOwnLogType() {
        for (LogType logType : LogType.values()) {
            for (Map.Entry<LogType, String> example : ROLLED_EXAMPLES.entrySet()) {
                boolean matches = RolledLogReader.rolledFilenamePattern(CLUSTER, logType).matcher(example.getValue()).matches();
                assertThat(logType + " matching " + example.getValue(), matches, is(example.getKey() == logType));
            }
        }
    }

    @Test
    public void testRolledFilenameMatchesBeforeCompression() {
        for (Map.Entry<LogType, String> example : ROLLED_EXAMPLES.entrySet()) {
            String uncompressed = example.getValue().substring(0, example.getValue().length() - ".gz".length());
            Matcher matcher = RolledLogReader.rolledFilenamePattern(CLUSTER, example.getKey()).matcher(uncompressed);
            assertThat(uncompressed, matcher.matches(), is(true));
            assertThat(uncompressed, matcher.group("gz"), nullValue());
        }
    }

    @Test
    public void testRolledFilenameCapturesDateAndIndex() {
        Matcher dated = RolledLogReader.rolledFilenamePattern(CLUSTER, LogType.AUDIT).matcher("test-cluster_audit-2026-10-18-12.json.gz");
        assertThat(dated.matches(), is(true));
        assertThat(dated.group("date"), is("2026-10-18"));
        assertThat(dated.group("index"), is("12"));
        assertThat(dated.group("gz"), is(".gz"));

        Matcher sizeOnly = RolledLogReader.rolledFilenamePattern(CLUSTER, LogType.DEPRECATION)
            .matcher("test-cluster_deprecation-7.json.gz");
        assertThat(sizeOnly.matches(), is(true));
        assertThat(sizeOnly.group("date"), nullValue());
        assertThat(sizeOnly.group("index"), is("7"));
    }

    @Test
    public void testRolledFilenameDoesNotMatchLiveFileOrOtherCluster() {
        for (LogType logType : LogType.values()) {
            var pattern = RolledLogReader.rolledFilenamePattern(CLUSTER, logType);
            for (LogType other : LogType.values()) {
                assertThat(other.resolveFilename(CLUSTER), pattern.matcher(other.resolveFilename(CLUSTER)).matches(), is(false));
            }
            String otherCluster = "other" + ROLLED_EXAMPLES.get(logType);
            assertThat(otherCluster, pattern.matcher(otherCluster).matches(), is(false));
            String tempFile = ROLLED_EXAMPLES.get(logType) + ".tmp";
            assertThat(tempFile, pattern.matcher(tempFile).matches(), is(false));
        }
    }

    @Test
    public void testRolledFilenameQuotesClusterName() {
        var pattern = RolledLogReader.rolledFilenamePattern("a.b", LogType.AUDIT);
        assertThat(pattern.matcher("a.b_audit-2026-10-18-1.json.gz").matches(), is(true));
        assertThat(pattern.matcher("aXb_audit-2026-10-18-1.json.gz").matches(), is(false));
    }
}
