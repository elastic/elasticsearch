/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Tuple;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class TimestampFormatFinderTests extends TextStructureTestCase {
    private static final boolean ECS_COMPATIBILITY_DISABLED = false;
    private static final boolean ECS_COMPATIBILITY_ENABLED = true;

    private static final Collection<Boolean> ecsCompatibilityModes = Arrays.asList(ECS_COMPATIBILITY_ENABLED, ECS_COMPATIBILITY_DISABLED);

    @SuppressWarnings("checkstyle:linelength")
    private static final String EXCEPTION_TRACE_SAMPLE =
        """
            [2018-02-28T14:49:40,517][DEBUG][o.e.a.b.TransportShardBulkAction] [an_index][2] failed to execute bulk item (index) BulkShardRequest [[an_index][2]] containing [33] requests
            java.lang.IllegalArgumentException: Document contains at least one immense term in field="message.keyword" (whose UTF8 encoding is longer than the max length 32766), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '[60, 83, 79, 65, 80, 45, 69, 78, 86, 58, 69, 110, 118, 101, 108, 111, 112, 101, 32, 120, 109, 108, 110, 115, 58, 83, 79, 65, 80, 45]...', original message: bytes can be at most 32766 in length; got 49023
                at org.apache.lucene.index.DefaultIndexingChain$PerField.invert(DefaultIndexingChain.java:796) ~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]
                at org.apache.lucene.index.DefaultIndexingChain.processField(DefaultIndexingChain.java:430) ~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]
                at org.apache.lucene.index.DefaultIndexingChain.processDocument(DefaultIndexingChain.java:392) ~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]
                at org.apache.lucene.index.DocumentsWriterPerThread.updateDocument(DocumentsWriterPerThread.java:240) ~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]
                at org.apache.lucene.index.DocumentsWriter.updateDocument(DocumentsWriter.java:496) ~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]
                at org.apache.lucene.index.IndexWriter.updateDocument(IndexWriter.java:1729) ~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]
                at org.apache.lucene.index.IndexWriter.addDocument(IndexWriter.java:1464) ~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]
                at org.elasticsearch.index.engine.InternalEngine.index(InternalEngine.java:1070) ~[elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.index.engine.InternalEngine.indexIntoLucene(InternalEngine.java:1012) ~[elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.index.engine.InternalEngine.index(InternalEngine.java:878) ~[elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.index.shard.IndexShard.index(IndexShard.java:738) ~[elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.index.shard.IndexShard.applyIndexOperation(IndexShard.java:707) ~[elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.index.shard.IndexShard.applyIndexOperationOnPrimary(IndexShard.java:673) ~[elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.bulk.TransportShardBulkAction.executeIndexRequestOnPrimary(TransportShardBulkAction.java:548) ~[elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.bulk.TransportShardBulkAction.executeIndexRequest(TransportShardBulkAction.java:140) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.bulk.TransportShardBulkAction.executeBulkItemRequest(TransportShardBulkAction.java:236) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.bulk.TransportShardBulkAction.performOnPrimary(TransportShardBulkAction.java:123) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.bulk.TransportShardBulkAction.shardOperationOnPrimary(TransportShardBulkAction.java:110) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.bulk.TransportShardBulkAction.shardOperationOnPrimary(TransportShardBulkAction.java:72) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryShardReference.perform(TransportReplicationAction.java:1034) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryShardReference.perform(TransportReplicationAction.java:1012) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.ReplicationOperation.execute(ReplicationOperation.java:103) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$AsyncPrimaryAction.onResponse(TransportReplicationAction.java:359) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$AsyncPrimaryAction.onResponse(TransportReplicationAction.java:299) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$1.onResponse(TransportReplicationAction.java:975) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$1.onResponse(TransportReplicationAction.java:972) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.index.shard.IndexShardOperationPermits.acquire(IndexShardOperationPermits.java:238) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.index.shard.IndexShard.acquirePrimaryOperationPermit(IndexShard.java:2220) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction.acquirePrimaryShardReference(TransportReplicationAction.java:984) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction.access$500(TransportReplicationAction.java:98) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$AsyncPrimaryAction.doRun(TransportReplicationAction.java:320) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:37) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryOperationTransportHandler.messageReceived(TransportReplicationAction.java:295) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryOperationTransportHandler.messageReceived(TransportReplicationAction.java:282) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.transport.RequestHandlerRegistry.processMessageReceived(RequestHandlerRegistry.java:66) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.transport.TransportService$7.doRun(TransportService.java:656) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.common.util.concurrent.ThreadContext$ContextPreservingAbstractRunnable.doRun(ThreadContext.java:635) [elasticsearch-6.2.1.jar:6.2.1]
                at org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:37) [elasticsearch-6.2.1.jar:6.2.1]
                at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_144]
                at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_144]
                at java.lang.Thread.run(Thread.java:748) [?:1.8.0_144]
            """;

    public void testValidOverrideFormatToGrokAndRegex() {

        assertEquals(
            new Tuple<>(
                "%{YEAR}-%{MONTHNUM2}-%{MONTHDAY}T%{HOUR}:%{MINUTE}:%{SECOND}%{ISO8601_TIMEZONE}",
                "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}(?:Z|[+-]\\d{4})\\b"
            ),
            TimestampFormatFinder.overrideFormatToGrokAndRegex("yyyy-MM-dd'T'HH:mm:ss,SSSXX")
        );
        assertEquals(
            new Tuple<>(
                "%{MONTHDAY}\\.%{MONTHNUM2}\\.%{YEAR} %{HOUR}:%{MINUTE} (?:AM|PM)",
                "\\b\\d{2}\\.\\d{2}\\.\\d{2} \\d{1,2}:\\d{2} [AP]M\\b"
            ),
            TimestampFormatFinder.overrideFormatToGrokAndRegex("dd.MM.yy h:mm a")
        );
        assertEquals(
            new Tuple<>(
                "%{MONTHNUM2}/%{MONTHDAY}/%{YEAR} %{HOUR}:%{MINUTE}:%{SECOND} %{TZ}",
                "\\b\\d{2}/\\d{2}/\\d{4} \\d{1,2}:\\d{2}:\\d{2} [A-Z]{3}\\b"
            ),
            TimestampFormatFinder.overrideFormatToGrokAndRegex("MM/dd/yyyy H:mm:ss zzz")
        );
    }

    public void testInvalidOverrideFormatToGrokAndRegex() {

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TimestampFormatFinder.overrideFormatToGrokAndRegex("MM/dd/yyyy\nH:mm:ss zzz")
        );
        assertEquals("Multi-line timestamp formats [MM/dd/yyyy\nH:mm:ss zzz] not supported", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> TimestampFormatFinder.overrideFormatToGrokAndRegex("MM/dd/YYYY H:mm:ss zzz")
        );
        assertEquals("Letter group [YYYY] in [MM/dd/YYYY H:mm:ss zzz] is not supported", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> TimestampFormatFinder.overrideFormatToGrokAndRegex("MM/dd/yyy H:mm:ss zzz"));
        assertEquals("Letter group [yyy] in [MM/dd/yyy H:mm:ss zzz] is not supported", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> TimestampFormatFinder.overrideFormatToGrokAndRegex("MM/dd/yyyy H:mm:ss+SSSSSS")
        );
        assertEquals(
            "Letter group [SSSSSS] in [MM/dd/yyyy H:mm:ss+SSSSSS] is not supported"
                + " because it is not preceded by [ss] and a separator from [:.,]",
            e.getMessage()
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> TimestampFormatFinder.overrideFormatToGrokAndRegex("MM/dd/yyyy H:mm,SSSSSS")
        );
        assertEquals(
            "Letter group [SSSSSS] in [MM/dd/yyyy H:mm,SSSSSS] is not supported"
                + " because it is not preceded by [ss] and a separator from [:.,]",
            e.getMessage()
        );
        e = expectThrows(IllegalArgumentException.class, () -> TimestampFormatFinder.overrideFormatToGrokAndRegex(" 'T' "));
        assertEquals("No time format letter groups in override format [ 'T' ]", e.getMessage());
    }

    public void testMakeCandidateFromOverrideFormat() {

        Consumer<Boolean> testMakeCandidateFromOverrideFormatGivenEcsCompatibility = (ecsCompatibility) -> {
            // Override is a special format
            assertSame(
                TimestampFormatFinder.ISO8601_CANDIDATE_FORMAT,
                TimestampFormatFinder.makeCandidateFromOverrideFormat("ISO8601", NOOP_TIMEOUT_CHECKER, ecsCompatibility)
            );
            assertSame(
                TimestampFormatFinder.UNIX_MS_CANDIDATE_FORMAT,
                TimestampFormatFinder.makeCandidateFromOverrideFormat("UNIX_MS", NOOP_TIMEOUT_CHECKER, ecsCompatibility)
            );
            assertSame(
                TimestampFormatFinder.UNIX_CANDIDATE_FORMAT,
                TimestampFormatFinder.makeCandidateFromOverrideFormat("UNIX", NOOP_TIMEOUT_CHECKER, ecsCompatibility)
            );
            assertSame(
                TimestampFormatFinder.TAI64N_CANDIDATE_FORMAT,
                TimestampFormatFinder.makeCandidateFromOverrideFormat("TAI64N", NOOP_TIMEOUT_CHECKER, ecsCompatibility)
            );

            // Override is covered by a built-in format
            TimestampFormatFinder.CandidateTimestampFormat candidate = TimestampFormatFinder.makeCandidateFromOverrideFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            assertEquals(TimestampFormatFinder.ISO8601_CANDIDATE_FORMAT.outputGrokPatternName, candidate.outputGrokPatternName);
            assertEquals(TimestampFormatFinder.ISO8601_CANDIDATE_FORMAT.strictGrokPattern, candidate.strictGrokPattern);
            // Can't compare Grok objects as Grok doesn't implement equals()
            assertEquals(TimestampFormatFinder.ISO8601_CANDIDATE_FORMAT.simplePattern.pattern(), candidate.simplePattern.pattern());
            // Exact format supplied is returned if it matches
            assertEquals(
                Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                candidate.javaTimestampFormatSupplier.apply("2018-05-15T16:14:56.374")
            );
            // Other supported formats are returned if exact format doesn't match
            assertEquals(Collections.singletonList("ISO8601"), candidate.javaTimestampFormatSupplier.apply("2018-05-15T16:14:56,374"));

            // Override is supported but not covered by any built-in format
            candidate = TimestampFormatFinder.makeCandidateFromOverrideFormat(
                "MM/dd/yyyy H:mm:ss zzz",
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            assertEquals(TimestampFormatFinder.CUSTOM_TIMESTAMP_GROK_NAME, candidate.outputGrokPatternName);
            assertEquals("%{MONTHNUM2}/%{MONTHDAY}/%{YEAR} %{HOUR}:%{MINUTE}:%{SECOND} %{TZ}", candidate.strictGrokPattern);
            assertEquals("\\b\\d{2}/\\d{2}/\\d{4} \\d{1,2}:\\d{2}:\\d{2} [A-Z]{3}\\b", candidate.simplePattern.pattern());
            assertEquals(
                Collections.singletonList("MM/dd/yyyy H:mm:ss zzz"),
                candidate.javaTimestampFormatSupplier.apply("05/15/2018 16:14:56 UTC")
            );

            candidate = TimestampFormatFinder.makeCandidateFromOverrideFormat(
                "M/d/yyyy H:mm:ss zzz",
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            assertEquals(TimestampFormatFinder.CUSTOM_TIMESTAMP_GROK_NAME, candidate.outputGrokPatternName);
            assertEquals("%{MONTHNUM}/%{MONTHDAY}/%{YEAR} %{HOUR}:%{MINUTE}:%{SECOND} %{TZ}", candidate.strictGrokPattern);
            assertEquals("\\b\\d{1,2}/\\d{1,2}/\\d{4} \\d{1,2}:\\d{2}:\\d{2} [A-Z]{3}\\b", candidate.simplePattern.pattern());
            assertEquals(
                Collections.singletonList("M/d/yyyy H:mm:ss zzz"),
                candidate.javaTimestampFormatSupplier.apply("5/15/2018 16:14:56 UTC")
            );
        };

        ecsCompatibilityModes.forEach(testMakeCandidateFromOverrideFormatGivenEcsCompatibility);
    }

    public void testRequiresTimezoneDependentParsing() {

        assertTrue(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("ISO8601", "2018-05-15T17:14:56"));
        assertFalse(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("ISO8601", "2018-05-15T17:14:56Z"));
        assertFalse(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("ISO8601", "2018-05-15T17:14:56-0100"));
        assertFalse(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("ISO8601", "2018-05-15T17:14:56+01:00"));

        assertFalse(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("UNIX_MS", "1526400896374"));
        assertFalse(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("UNIX", "1526400896"));
        assertFalse(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("TAI64N", "400000005afb078a164ac980"));

        assertFalse(
            TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing(
                "EEE, dd MMM yyyy HH:mm:ss XXX",
                "Tue, 15 May 2018 17:14:56 +01:00"
            )
        );
        assertTrue(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("yyyyMMddHHmmss", "20180515171456"));
        assertFalse(
            TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing(
                "EEE MMM dd yy HH:mm:ss zzz",
                "Tue May 15 18 16:14:56 UTC"
            )
        );
        assertFalse(
            TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing(
                "yyyy-MM-dd HH:mm:ss,SSS XX",
                "2018-05-15 17:14:56,374 +0100"
            )
        );
        assertTrue(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("MMM dd HH:mm:ss.SSS", "May 15 17:14:56.725"));

        assertTrue(
            TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("yyyy.MM.dd'zXz'HH:mm:ss", "2018.05.15zXz17:14:56")
        );
        assertTrue(TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("yyyy.MM.dd HH:mm:ss'z'", "2018.05.15 17:14:56z"));
        assertTrue(
            TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("'XX'yyyy.MM.dd HH:mm:ss", "XX2018.05.15 17:14:56")
        );
        assertFalse(
            TimestampFormatFinder.TimestampMatch.requiresTimezoneDependentParsing("'XX'yyyy.MM.dd HH:mm:ssXX", "XX2018.05.15 17:14:56Z")
        );
    }

    public void testMatchHasNanosecondPrecision() {

        assertFalse(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56Z"));
        assertFalse(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56-0100"));
        assertFalse(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56+01:00"));
        assertFalse(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56,374Z"));
        assertFalse(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56.374+0100"));
        assertFalse(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56,374-01:00"));
        assertTrue(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56.374123Z"));
        assertTrue(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56,374123-0100"));
        assertTrue(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56.374123+01:00"));
        assertTrue(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56,374123456Z"));
        assertTrue(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56.374123456+0100"));
        assertTrue(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("ISO8601", "2018-05-15T17:14:56,374123456-01:00"));

        assertFalse(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("UNIX_MS", "1526400896374"));
        assertFalse(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("UNIX", "1526400896"));
        assertTrue(TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("TAI64N", "400000005afb078a164ac980"));

        assertFalse(
            TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("yyyy-MM-dd HH:mm:ss,SSS XX", "2018-05-15 17:14:56,374 +0100")
        );
        assertTrue(
            TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision(
                "yyyy-MM-dd HH:mm:ss.SSSSSS XX",
                "2018-05-15 17:14:56.374123 +0100"
            )
        );
        assertTrue(
            TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision(
                "yyyy-MM-dd HH:mm:ss,SSSSSSSSS XX",
                "2018-05-15 17:14:56,374123456 +0100"
            )
        );

        assertFalse(
            TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("'SSSS'yyyy.MM.dd HH:mm:ssXX", "SSSS2018.05.15 17:14:56Z")
        );
        assertFalse(
            TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("yyyy.MM.dd HH:mm:ss,SSS'SSSS'", "2018.05.15 17:14:56,374SSSS")
        );
        assertTrue(
            TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision(
                "yyyy.MM.dd HH:mm:ss,SSSS'SSSS'",
                "2018.05.15 17:14:56,3741SSSS"
            )
        );
        assertFalse(
            TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("yyyy.MM.dd'SSSS'HH:mm:ss.SSS", "2018.05.15SSSS17:14:56.374")
        );
        assertTrue(
            TimestampFormatFinder.TimestampMatch.matchHasNanosecondPrecision("yyyy.MM.dd'SSSS'HH:mm:ss.SSSS", "2018.05.15SSSS17:14:56.3741")
        );
    }

    public void testParseIndeterminateDateNumbers() {

        // Simplest case - nothing is indeterminate
        int[] indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "2018-05-15T16:14:56,374Z",
            Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss,SSSXX")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(-1, indeterminateDateNumbers[0]);
        assertEquals(-1, indeterminateDateNumbers[1]);

        // US with padding
        indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "05/15/2018 16:14:56",
            Collections.singletonList("??/??/yyyy HH:mm:ss")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(5, indeterminateDateNumbers[0]);
        assertEquals(15, indeterminateDateNumbers[1]);

        // US with padding, 2 digit year
        indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "05/15/18 16:14:56",
            Collections.singletonList("??/??/yy HH:mm:ss")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(5, indeterminateDateNumbers[0]);
        assertEquals(15, indeterminateDateNumbers[1]);

        // US without padding
        indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "5/15/2018 16:14:56",
            Collections.singletonList("?/?/yyyy HH:mm:ss")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(5, indeterminateDateNumbers[0]);
        assertEquals(15, indeterminateDateNumbers[1]);

        // US without padding, 2 digit year
        indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "5/15/18 16:14:56",
            Collections.singletonList("?/?/yy HH:mm:ss")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(5, indeterminateDateNumbers[0]);
        assertEquals(15, indeterminateDateNumbers[1]);

        // EU with padding
        indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "15/05/2018 16:14:56",
            Collections.singletonList("??/??/yyyy HH:mm:ss")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(15, indeterminateDateNumbers[0]);
        assertEquals(5, indeterminateDateNumbers[1]);

        // EU with padding, 2 digit year
        indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "15/05/18 16:14:56",
            Collections.singletonList("??/??/yy HH:mm:ss")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(15, indeterminateDateNumbers[0]);
        assertEquals(5, indeterminateDateNumbers[1]);

        // EU without padding
        indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "15/5/2018 16:14:56",
            Collections.singletonList("?/?/yyyy HH:mm:ss")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(15, indeterminateDateNumbers[0]);
        assertEquals(5, indeterminateDateNumbers[1]);

        // EU without padding, 2 digit year
        indeterminateDateNumbers = TimestampFormatFinder.TimestampMatch.parseIndeterminateDateNumbers(
            "15/5/18 16:14:56",
            Collections.singletonList("?/?/yy HH:mm:ss")
        );
        assertEquals(2, indeterminateDateNumbers.length);
        assertEquals(15, indeterminateDateNumbers[0]);
        assertEquals(5, indeterminateDateNumbers[1]);
    }

    public void testDeterminiseJavaTimestampFormat() {

        // Indeterminate at the beginning of the pattern
        assertEquals("dd/MM/yyyy HH:mm:ss", TimestampFormatFinder.determiniseJavaTimestampFormat("??/??/yyyy HH:mm:ss", true));
        assertEquals("MM/dd/yyyy HH:mm:ss", TimestampFormatFinder.determiniseJavaTimestampFormat("??/??/yyyy HH:mm:ss", false));
        assertEquals("d/M/yyyy HH:mm:ss", TimestampFormatFinder.determiniseJavaTimestampFormat("?/?/yyyy HH:mm:ss", true));
        assertEquals("M/d/yyyy HH:mm:ss", TimestampFormatFinder.determiniseJavaTimestampFormat("?/?/yyyy HH:mm:ss", false));
        // Indeterminate in the middle of the pattern
        assertEquals("HH:mm:ss dd/MM/yyyy", TimestampFormatFinder.determiniseJavaTimestampFormat("HH:mm:ss ??/??/yyyy", true));
        assertEquals("HH:mm:ss MM/dd/yyyy", TimestampFormatFinder.determiniseJavaTimestampFormat("HH:mm:ss ??/??/yyyy", false));
        assertEquals("HH:mm:ss d/M/yyyy", TimestampFormatFinder.determiniseJavaTimestampFormat("HH:mm:ss ?/?/yyyy", true));
        assertEquals("HH:mm:ss M/d/yyyy", TimestampFormatFinder.determiniseJavaTimestampFormat("HH:mm:ss ?/?/yyyy", false));
        // No separators
        assertEquals("ddMMyyyyHHmmss", TimestampFormatFinder.determiniseJavaTimestampFormat("????yyyyHHmmss", true));
        assertEquals("MMddyyyyHHmmss", TimestampFormatFinder.determiniseJavaTimestampFormat("????yyyyHHmmss", false));
        // It's unreasonable to expect a variable length format like 'd' or 'M' to work without separators
    }

    public void testGuessIsDayFirstFromFormats() {

        TimestampFormatFinder timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/5/2018 16:14:56");
        timestampFormatFinder.addSample("06/6/2018 17:14:56");
        timestampFormatFinder.addSample("07/7/2018 18:14:56");

        // This is based on the fact that %{MONTHNUM} can match a single digit whereas %{MONTHDAY} cannot
        assertTrue(timestampFormatFinder.guessIsDayFirstFromFormats(timestampFormatFinder.getRawJavaTimestampFormats()));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/5/18 16:14:56");
        timestampFormatFinder.addSample("06/6/18 17:14:56");
        timestampFormatFinder.addSample("07/7/18 18:14:56");

        assertTrue(timestampFormatFinder.guessIsDayFirstFromFormats(timestampFormatFinder.getRawJavaTimestampFormats()));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("5/05/2018 16:14:56");
        timestampFormatFinder.addSample("6/06/2018 17:14:56");
        timestampFormatFinder.addSample("7/07/2018 18:14:56");

        // This is based on the fact that %{MONTHNUM} can match a single digit whereas %{MONTHDAY} cannot
        assertFalse(timestampFormatFinder.guessIsDayFirstFromFormats(timestampFormatFinder.getRawJavaTimestampFormats()));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("5/05/18 16:14:56");
        timestampFormatFinder.addSample("6/06/18 17:14:56");
        timestampFormatFinder.addSample("7/07/18 18:14:56");

        assertFalse(timestampFormatFinder.guessIsDayFirstFromFormats(timestampFormatFinder.getRawJavaTimestampFormats()));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("5/05/2018 16:14:56");
        timestampFormatFinder.addSample("06/6/2018 17:14:56");
        timestampFormatFinder.addSample("7/07/2018 18:14:56");

        // Inconsistent so no decision
        assertNull(timestampFormatFinder.guessIsDayFirstFromFormats(timestampFormatFinder.getRawJavaTimestampFormats()));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("5/05/18 16:14:56");
        timestampFormatFinder.addSample("06/6/18 17:14:56");
        timestampFormatFinder.addSample("7/07/18 18:14:56");

        assertNull(timestampFormatFinder.guessIsDayFirstFromFormats(timestampFormatFinder.getRawJavaTimestampFormats()));
    }

    public void testGuessIsDayFirstFromMatchesSingleFormat() {

        TimestampFormatFinder timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("05/15/2018 17:14:56");
        timestampFormatFinder.addSample("05/25/2018 18:14:56");

        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("05/15/18 17:14:56");
        timestampFormatFinder.addSample("05/25/18 18:14:56");

        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("15/05/2018 17:14:56");
        timestampFormatFinder.addSample("25/05/2018 18:14:56");

        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("15/05/18 17:14:56");
        timestampFormatFinder.addSample("25/05/18 18:14:56");

        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("05/06/2018 17:14:56");
        timestampFormatFinder.addSample("05/07/2018 18:14:56");

        // Second number has 3 values, first only 1, so guess second is day
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("05/06/18 17:14:56");
        timestampFormatFinder.addSample("05/07/18 18:14:56");

        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("06/05/2018 17:14:56");
        timestampFormatFinder.addSample("07/05/2018 18:14:56");

        // First number has 3 values, second only 1, so guess first is day
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("06/05/18 17:14:56");
        timestampFormatFinder.addSample("07/05/18 18:14:56");

        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("06/06/2018 17:14:56");
        timestampFormatFinder.addSample("07/07/2018 18:14:56");

        // Insufficient evidence to decide
        assertNull(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertNull(timestampFormatFinder.guessIsDayFirstFromMatches(null));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("06/06/18 17:14:56");
        timestampFormatFinder.addSample("07/07/18 18:14:56");

        assertNull(timestampFormatFinder.guessIsDayFirstFromMatches(null));
        assertNull(timestampFormatFinder.guessIsDayFirstFromMatches(null));
    }

    public void testGuessIsDayFirstFromMatchesMultipleFormats() {

        // Similar to the test above, but with the possibility that the secondary
        // ISO8601 formats cause confusion - this test proves that they don't

        // DATESTAMP supports both 2 and 4 digit years, so each test is repeated for both lengths
        TimestampFormatFinder.TimestampFormat expectedPrimaryFormat = new TimestampFormatFinder.TimestampFormat(
            Collections.singletonList("??/??/yyyy HH:mm:ss"),
            Pattern.compile("\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b"),
            "DATESTAMP",
            Collections.emptyMap(),
            ""
        );

        TimestampFormatFinder timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("2018-05-15T17:14:56");
        timestampFormatFinder.addSample("05/15/2018 17:14:56");
        timestampFormatFinder.addSample("2018-05-25T18:14:56");
        timestampFormatFinder.addSample("05/25/2018 18:14:56");

        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("2018-05-15T17:14:56");
        timestampFormatFinder.addSample("05/15/18 17:14:56");
        timestampFormatFinder.addSample("2018-05-25T18:14:56");
        timestampFormatFinder.addSample("05/25/18 18:14:56");

        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("2018-05-15T17:14:56");
        timestampFormatFinder.addSample("15/05/2018 17:14:56");
        timestampFormatFinder.addSample("2018-05-25T18:14:56");
        timestampFormatFinder.addSample("25/05/2018 18:14:56");

        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("2018-05-15T17:14:56");
        timestampFormatFinder.addSample("15/05/18 17:14:56");
        timestampFormatFinder.addSample("2018-05-25T18:14:56");
        timestampFormatFinder.addSample("25/05/18 18:14:56");

        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("2018-05-06T17:14:56");
        timestampFormatFinder.addSample("05/06/2018 17:14:56");
        timestampFormatFinder.addSample("2018-05-07T18:14:56");
        timestampFormatFinder.addSample("05/07/2018 18:14:56");

        // Second number has 3 values, first only 1, so guess second is day
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("2018-05-06T17:14:56");
        timestampFormatFinder.addSample("05/06/18 17:14:56");
        timestampFormatFinder.addSample("2018-05-07T18:14:56");
        timestampFormatFinder.addSample("05/07/18 18:14:56");

        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("2018-05-06T17:14:56");
        timestampFormatFinder.addSample("06/05/2018 17:14:56");
        timestampFormatFinder.addSample("2018-05-07T18:14:56");
        timestampFormatFinder.addSample("07/05/2018 18:14:56");

        // First number has 3 values, second only 1, so guess first is day
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("2018-05-06T17:14:56");
        timestampFormatFinder.addSample("06/05/18 17:14:56");
        timestampFormatFinder.addSample("2018-05-07T18:14:56");
        timestampFormatFinder.addSample("07/05/18 18:14:56");

        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/2018 16:14:56");
        timestampFormatFinder.addSample("2018-06-06T17:14:56");
        timestampFormatFinder.addSample("06/06/2018 17:14:56");
        timestampFormatFinder.addSample("2018-07-07T18:14:56");
        timestampFormatFinder.addSample("07/07/2018 18:14:56");

        // Insufficient evidence to decide
        assertNull(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertNull(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));

        timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, false, NOOP_TIMEOUT_CHECKER);

        timestampFormatFinder.addSample("05/05/18 16:14:56");
        timestampFormatFinder.addSample("2018-06-06T17:14:56");
        timestampFormatFinder.addSample("06/06/18 17:14:56");
        timestampFormatFinder.addSample("2018-07-07T18:14:56");
        timestampFormatFinder.addSample("07/07/18 18:14:56");

        assertNull(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
        assertNull(timestampFormatFinder.guessIsDayFirstFromMatches(expectedPrimaryFormat));
    }

    public void testGuessIsDayFirstFromLocale() {

        TimestampFormatFinder timestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);

        // Locale fallback is the only way to decide

        // The US locale should work for both FIPS and non-FIPS
        assertFalse(timestampFormatFinder.guessIsDayFirstFromLocale(Locale.US));

        // Non-US locales may not work correctly in a FIPS JVM - see https://github.com/elastic/elasticsearch/issues/45140
        assumeFalse("Locales not reliable in FIPS JVM", inFipsJvm());

        assertTrue(timestampFormatFinder.guessIsDayFirstFromLocale(Locale.UK));
        assertTrue(timestampFormatFinder.guessIsDayFirstFromLocale(Locale.FRANCE));
        assertFalse(timestampFormatFinder.guessIsDayFirstFromLocale(Locale.JAPAN));
    }

    public void testStringToNumberPosBitSet() {

        BitSet bitSet = TimestampFormatFinder.stringToNumberPosBitSet("");
        assertTrue(bitSet.isEmpty());
        assertEquals(0, bitSet.length());

        bitSet = TimestampFormatFinder.stringToNumberPosBitSet(" 1");
        assertEquals(2, bitSet.length());
        assertFalse(bitSet.get(0));
        assertTrue(bitSet.get(1));

        bitSet = TimestampFormatFinder.stringToNumberPosBitSet("1 1 1");
        assertEquals(5, bitSet.length());
        assertTrue(bitSet.get(0));
        assertFalse(bitSet.get(1));
        assertTrue(bitSet.get(2));
        assertFalse(bitSet.get(3));
        assertTrue(bitSet.get(4));

        bitSet = TimestampFormatFinder.stringToNumberPosBitSet("05/05/2018 16:14:56");
        assertEquals(19, bitSet.length());
        assertTrue(bitSet.get(0));
        assertTrue(bitSet.get(1));
        assertFalse(bitSet.get(2));
        assertTrue(bitSet.get(3));
        assertTrue(bitSet.get(4));
        assertFalse(bitSet.get(5));
        assertTrue(bitSet.get(6));
        assertTrue(bitSet.get(7));
        assertTrue(bitSet.get(8));
        assertTrue(bitSet.get(9));
        assertFalse(bitSet.get(10));
        assertTrue(bitSet.get(11));
        assertTrue(bitSet.get(12));
        assertFalse(bitSet.get(13));
        assertTrue(bitSet.get(14));
        assertTrue(bitSet.get(15));
        assertFalse(bitSet.get(16));
        assertTrue(bitSet.get(17));
        assertTrue(bitSet.get(18));
    }

    public void testFindBitPattern() {

        BitSet findIn = TimestampFormatFinder.stringToNumberPosBitSet("");
        BitSet toFind = TimestampFormatFinder.stringToNumberPosBitSet("");
        assertEquals(0, TimestampFormatFinder.findBitPattern(findIn, 0, toFind));

        findIn = TimestampFormatFinder.stringToNumberPosBitSet("1 1 1");
        toFind = TimestampFormatFinder.stringToNumberPosBitSet("");
        assertEquals(0, TimestampFormatFinder.findBitPattern(findIn, 0, toFind));
        assertEquals(1, TimestampFormatFinder.findBitPattern(findIn, 1, toFind));
        assertEquals(2, TimestampFormatFinder.findBitPattern(findIn, 2, toFind));

        findIn = TimestampFormatFinder.stringToNumberPosBitSet("1 1 1");
        toFind = TimestampFormatFinder.stringToNumberPosBitSet("1");
        assertEquals(0, TimestampFormatFinder.findBitPattern(findIn, 0, toFind));
        assertEquals(2, TimestampFormatFinder.findBitPattern(findIn, 1, toFind));
        assertEquals(2, TimestampFormatFinder.findBitPattern(findIn, 2, toFind));

        findIn = TimestampFormatFinder.stringToNumberPosBitSet("1 1 1");
        toFind = TimestampFormatFinder.stringToNumberPosBitSet(" 1");
        assertEquals(1, TimestampFormatFinder.findBitPattern(findIn, 0, toFind));
        assertEquals(1, TimestampFormatFinder.findBitPattern(findIn, 1, toFind));
        assertEquals(3, TimestampFormatFinder.findBitPattern(findIn, 2, toFind));

        findIn = TimestampFormatFinder.stringToNumberPosBitSet("1 1 1");
        toFind = TimestampFormatFinder.stringToNumberPosBitSet("1 1");
        assertEquals(0, TimestampFormatFinder.findBitPattern(findIn, 0, toFind));
        assertEquals(2, TimestampFormatFinder.findBitPattern(findIn, 1, toFind));
        assertEquals(2, TimestampFormatFinder.findBitPattern(findIn, 2, toFind));
        assertEquals(-1, TimestampFormatFinder.findBitPattern(findIn, 3, toFind));

        findIn = TimestampFormatFinder.stringToNumberPosBitSet("1 11 1 1");
        toFind = TimestampFormatFinder.stringToNumberPosBitSet("11 1");
        assertEquals(2, TimestampFormatFinder.findBitPattern(findIn, 0, toFind));
        assertEquals(2, TimestampFormatFinder.findBitPattern(findIn, 1, toFind));
        assertEquals(2, TimestampFormatFinder.findBitPattern(findIn, 2, toFind));
        assertEquals(-1, TimestampFormatFinder.findBitPattern(findIn, 3, toFind));

        findIn = TimestampFormatFinder.stringToNumberPosBitSet("1 11 1 1");
        toFind = TimestampFormatFinder.stringToNumberPosBitSet(" 11 1");
        assertEquals(1, TimestampFormatFinder.findBitPattern(findIn, 0, toFind));
        assertEquals(1, TimestampFormatFinder.findBitPattern(findIn, 1, toFind));
        assertEquals(-1, TimestampFormatFinder.findBitPattern(findIn, 2, toFind));

        findIn = TimestampFormatFinder.stringToNumberPosBitSet("1 11 1 1");
        toFind = TimestampFormatFinder.stringToNumberPosBitSet(" 1 1");
        assertEquals(4, TimestampFormatFinder.findBitPattern(findIn, 0, toFind));
        assertEquals(4, TimestampFormatFinder.findBitPattern(findIn, 4, toFind));
        assertEquals(-1, TimestampFormatFinder.findBitPattern(findIn, 5, toFind));
    }

    public void testFindBoundsForCandidate() {

        final TimestampFormatFinder.CandidateTimestampFormat httpdCandidateFormat = TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS.stream()
            .filter(candidate -> candidate.outputGrokPatternName.equals("HTTPDATE"))
            .findAny()
            .get();

        BitSet numberPosBitSet = TimestampFormatFinder.stringToNumberPosBitSet(
            "[2018-05-11T17:07:29,553][INFO ]"
                + "[o.e.e.NodeEnvironment    ] [node-0] heap size [3.9gb], compressed ordinary object pointers [true]"
        );
        assertEquals(
            new Tuple<>(1, 36),
            TimestampFormatFinder.findBoundsForCandidate(TimestampFormatFinder.ISO8601_CANDIDATE_FORMAT, numberPosBitSet)
        );
        assertEquals(new Tuple<>(-1, -1), TimestampFormatFinder.findBoundsForCandidate(httpdCandidateFormat, numberPosBitSet));
        // TAI64N doesn't necessarily contain digits, so this functionality cannot guarantee that it won't match somewhere in the text
        assertEquals(
            new Tuple<>(0, Integer.MAX_VALUE),
            TimestampFormatFinder.findBoundsForCandidate(TimestampFormatFinder.TAI64N_CANDIDATE_FORMAT, numberPosBitSet)
        );

        numberPosBitSet = TimestampFormatFinder.stringToNumberPosBitSet("""
            192.168.62.101 - - [29/Jun/2016:12:11:31 +0000] "POST //apiserv:8080/engine/v2/jobs HTTP/1.1" 201 42 "-" "curl/7.46.0" 384""");
        assertEquals(
            new Tuple<>(-1, -1),
            TimestampFormatFinder.findBoundsForCandidate(TimestampFormatFinder.ISO8601_CANDIDATE_FORMAT, numberPosBitSet)
        );
        assertEquals(new Tuple<>(20, 46), TimestampFormatFinder.findBoundsForCandidate(httpdCandidateFormat, numberPosBitSet));
        assertEquals(
            new Tuple<>(0, Integer.MAX_VALUE),
            TimestampFormatFinder.findBoundsForCandidate(TimestampFormatFinder.TAI64N_CANDIDATE_FORMAT, numberPosBitSet)
        );
    }

    public void testFindFormatGivenNoMatch() {

        validateNoTimestampMatch("");
        validateNoTimestampMatch("no timestamps in here");
        validateNoTimestampMatch(":::");
        validateNoTimestampMatch("/+");
        // These two don't match because they're too far in the future
        // when interpreted as seconds/milliseconds from the epoch
        // (they need to be 10 or 13 digits beginning with 1 or 2)
        validateNoTimestampMatch("3213213213");
        validateNoTimestampMatch("9789522792167");
    }

    public void testFindFormatGivenOnlyIso8601() {

        Consumer<Boolean> testFindFormatGivenOnlyIso8601AndEcsCompatibility = (ecsCompatibility) -> {
            validateTimestampMatch(
                "2018-05-15T16:14:56,374Z",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14:56,374+0100",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14:56,374+01:00",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14:56,374",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400896374L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "2018-05-15T16:14:56Z",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14:56+0100",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14:56+01:00",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14:56",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "2018-05-15T16:14Z",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400840000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14+0100",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400840000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14+01:00",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400840000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15T17:14",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "ISO8601",
                1526400840000L,
                ecsCompatibility
            );

            // TIMESTAMP_ISO8601 doesn't match ISO8601 if it's only a date with no time
            validateTimestampMatch(
                "2018-05-15",
                "CUSTOM_TIMESTAMP",
                "\\b\\d{4}-\\d{2}-\\d{2}\\b",
                "ISO8601",
                1526338800000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "2018-05-15 16:14:56,374Z",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm:ss,SSSXX",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14:56,374+0100",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm:ss,SSSXX",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14:56,374+01:00",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm:ss,SSSXXX",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14:56,374",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm:ss,SSS",
                1526400896374L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "2018-05-15 16:14:56Z",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm:ssXX",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14:56+0100",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm:ssXX",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14:56+01:00",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm:ssXXX",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14:56",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm:ss",
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "2018-05-15 16:14Z",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mmXX",
                1526400840000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14+0100",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mmXX",
                1526400840000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14+01:00",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mmXXX",
                1526400840000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "2018-05-15 17:14",
                "TIMESTAMP_ISO8601",
                "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
                "yyyy-MM-dd HH:mm",
                1526400840000L,
                ecsCompatibility
            );
        };

        ecsCompatibilityModes.forEach(testFindFormatGivenOnlyIso8601AndEcsCompatibility);

    }

    public void testFindFormatGivenOnlyKnownTimestampFormat() {

        // Note: some of the time formats give millisecond accuracy, some second accuracy and some minute accuracy

        Consumer<Boolean> testFindFormatGivenOnlyKnownTimestampFormatAndEcsCompatibility = (ecsCompatibility) -> {
            validateTimestampMatch(
                "2018-05-15 17:14:56,374 +0100",
                "TOMCAT_DATESTAMP",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[:.,]\\d{3}",
                "yyyy-MM-dd HH:mm:ss,SSS XX",
                1526400896374L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "Tue May 15 18 16:14:56 UTC",
                "DATESTAMP_RFC822",
                "\\b[A-Z]\\S{2} [A-Z]\\S{2} \\d{1,2} \\d{2} \\d{2}:\\d{2}:\\d{2}\\b",
                Arrays.asList("EEE MMM dd yy HH:mm:ss zzz", "EEE MMM d yy HH:mm:ss zzz"),
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "Tue, 15 May 2018 17:14:56 +01:00",
                "DATESTAMP_RFC2822",
                "\\b[A-Z]\\S{2}, \\d{1,2} [A-Z]\\S{2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b",
                "EEE, dd MMM yyyy HH:mm:ss XXX",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "Tue, 15 May 2018 17:14:56 +0100",
                "DATESTAMP_RFC2822",
                "\\b[A-Z]\\S{2}, \\d{1,2} [A-Z]\\S{2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b",
                "EEE, dd MMM yyyy HH:mm:ss XX",
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "Tue May 15 16:14:56 UTC 2018",
                "DATESTAMP_OTHER",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b",
                Arrays.asList("EEE MMM dd HH:mm:ss zzz yyyy", "EEE MMM d HH:mm:ss zzz yyyy"),
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "20180515171456",
                "DATESTAMP_EVENTLOG",
                "\\b\\d{14}\\b",
                "yyyyMMddHHmmss",
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "Tue May 15 17:14:56 2018",
                "HTTPDERROR_DATE",
                "\\b[A-Z]\\S{2} [A-Z]\\S{2} \\d{2} \\d{2}:\\d{2}:\\d{2} \\d{4}\\b",
                "EEE MMM dd HH:mm:ss yyyy",
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "May 15 17:14:56.725",
                "SYSLOGTIMESTAMP",
                "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b",
                Arrays.asList("MMM dd HH:mm:ss.SSS", "MMM  d HH:mm:ss.SSS", "MMM d HH:mm:ss.SSS"),
                1526400896725L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "May 15 17:14:56",
                "SYSLOGTIMESTAMP",
                "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b",
                Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss", "MMM d HH:mm:ss"),
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "15/May/2018:17:14:56 +0100",
                "HTTPDATE",
                "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ",
                "dd/MMM/yyyy:HH:mm:ss XX",
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "May 15, 2018 5:14:56 PM",
                "CATALINA_DATESTAMP",
                "\\b[A-Z]\\S{2} \\d{2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b",
                "MMM dd, yyyy h:mm:ss a",
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "May 15 2018 17:14:56",
                "CISCOTIMESTAMP",
                "\\b[A-Z]\\S{2} {1,2}\\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b",
                Arrays.asList("MMM dd yyyy HH:mm:ss", "MMM  d yyyy HH:mm:ss", "MMM d yyyy HH:mm:ss"),
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "05/15/2018 17:14:56,374",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "MM/dd/yyyy HH:mm:ss,SSS",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "05-15-2018-17:14:56.374",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "MM-dd-yyyy-HH:mm:ss.SSS",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15/05/2018 17:14:56.374",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "dd/MM/yyyy HH:mm:ss.SSS",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15-05-2018-17:14:56,374",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "dd-MM-yyyy-HH:mm:ss,SSS",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15.05.2018 17:14:56.374",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "dd.MM.yyyy HH:mm:ss.SSS",
                1526400896374L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "05/15/2018 17:14:56",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "MM/dd/yyyy HH:mm:ss",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "05-15-2018-17:14:56",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "MM-dd-yyyy-HH:mm:ss",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15/05/2018 17:14:56",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "dd/MM/yyyy HH:mm:ss",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15-05-2018-17:14:56",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "dd-MM-yyyy-HH:mm:ss",
                1526400896000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15.05.2018 17:14:56",
                "DATESTAMP",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
                "dd.MM.yyyy HH:mm:ss",
                1526400896000L,
                ecsCompatibility
            );

            validateTimestampMatch(
                "05/15/2018",
                "DATE",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}\\b",
                "MM/dd/yyyy",
                1526338800000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "05-15-2018",
                "DATE",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}\\b",
                "MM-dd-yyyy",
                1526338800000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15/05/2018",
                "DATE",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}\\b",
                "dd/MM/yyyy",
                1526338800000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15-05-2018",
                "DATE",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}\\b",
                "dd-MM-yyyy",
                1526338800000L,
                ecsCompatibility
            );
            validateTimestampMatch(
                "15.05.2018",
                "DATE",
                "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}\\b",
                "dd.MM.yyyy",
                1526338800000L,
                ecsCompatibility
            );

            // The Kibana export format doesn't correspond to a built-in Grok pattern, so it has to be custom
            validateTimestampMatch(
                "May 15, 2018 @ 17:14:56.374",
                "CUSTOM_TIMESTAMP",
                "\\b[A-Z]\\S{2} \\d{1,2}, \\d{4} @ \\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\b",
                "MMM d, yyyy @ HH:mm:ss.SSS",
                1526400896374L,
                ecsCompatibility
            );
        };

        ecsCompatibilityModes.forEach(testFindFormatGivenOnlyKnownTimestampFormatAndEcsCompatibility);
    }

    public void testFindFormatGivenOnlySystemDate() {

        Consumer<Boolean> testFindFormatGivenOnlySystemDateAndEcsCompatibility = (ecsCompatibility) -> {
            validateTimestampMatch("1000000000000", "POSINT", "\\b\\d{13}\\b", "UNIX_MS", 1000000000000L, ecsCompatibility);
            validateTimestampMatch("1526400896374", "POSINT", "\\b\\d{13}\\b", "UNIX_MS", 1526400896374L, ecsCompatibility);
            validateTimestampMatch("2999999999999", "POSINT", "\\b\\d{13}\\b", "UNIX_MS", 2999999999999L, ecsCompatibility);

            validateTimestampMatch("1000000000", "NUMBER", "\\b\\d{10}\\b", "UNIX", 1000000000000L, ecsCompatibility);
            validateTimestampMatch("1526400896.736", "NUMBER", "\\b\\d{10}\\b", "UNIX", 1526400896736L, ecsCompatibility);
            validateTimestampMatch("1526400896", "NUMBER", "\\b\\d{10}\\b", "UNIX", 1526400896000L, ecsCompatibility);
            validateTimestampMatch("2999999999.999", "NUMBER", "\\b\\d{10}\\b", "UNIX", 2999999999999L, ecsCompatibility);

            validateTimestampMatch(
                "400000005afb078a164ac980",
                "BASE16NUM",
                "\\b[0-9A-Fa-f]{24}\\b",
                "TAI64N",
                1526400896374L,
                ecsCompatibility
            );
        };

        ecsCompatibilityModes.forEach(testFindFormatGivenOnlySystemDateAndEcsCompatibility);

    }

    public void testCustomOverrideMatchingBuiltInFormat() {

        String overrideFormat = "yyyy-MM-dd HH:mm:ss,SSS";
        String text = "2018-05-15 17:14:56,374";
        String expectedSimpleRegex = "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}";
        String expectedGrokPatternName = "TIMESTAMP_ISO8601";

        Consumer<Boolean> testCustomOverrideMatchingBuiltInFormatGivenEcsCompatibility = (ecsCompatibility) -> {
            TimestampFormatFinder strictTimestampFormatFinder = new TimestampFormatFinder(
                explanation,
                overrideFormat,
                true,
                true,
                true,
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            strictTimestampFormatFinder.addSample(text);
            assertEquals(expectedGrokPatternName, strictTimestampFormatFinder.getGrokPatternName());
            assertEquals(Collections.emptyMap(), strictTimestampFormatFinder.getCustomGrokPatternDefinitions());
            assertEquals(expectedSimpleRegex, strictTimestampFormatFinder.getSimplePattern().pattern());
            assertEquals(Collections.singletonList(overrideFormat), strictTimestampFormatFinder.getJavaTimestampFormats());
            assertEquals(1, strictTimestampFormatFinder.getNumMatchedFormats());

            TimestampFormatFinder lenientTimestampFormatFinder = new TimestampFormatFinder(
                explanation,
                overrideFormat,
                false,
                false,
                false,
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            lenientTimestampFormatFinder.addSample(text);
            lenientTimestampFormatFinder.selectBestMatch();
            assertEquals(expectedGrokPatternName, lenientTimestampFormatFinder.getGrokPatternName());
            assertEquals(Collections.emptyMap(), lenientTimestampFormatFinder.getCustomGrokPatternDefinitions());
            assertEquals(expectedSimpleRegex, lenientTimestampFormatFinder.getSimplePattern().pattern());
            assertEquals(Collections.singletonList(overrideFormat), lenientTimestampFormatFinder.getJavaTimestampFormats());
            assertEquals(1, lenientTimestampFormatFinder.getNumMatchedFormats());
        };

        ecsCompatibilityModes.forEach(testCustomOverrideMatchingBuiltInFormatGivenEcsCompatibility);
    }

    public void testCustomOverridesNotMatchingBuiltInFormat() {

        validateCustomOverrideNotMatchingBuiltInFormat(
            "MM/dd HH.mm.ss,SSSSSS 'in' yyyy",
            "05/15 17.14.56,374946 in 2018",
            "\\b\\d{2}/\\d{2} \\d{2}\\.\\d{2}\\.\\d{2},\\d{6} in \\d{4}\\b",
            "CUSTOM_TIMESTAMP",
            Collections.singletonMap(
                TimestampFormatFinder.CUSTOM_TIMESTAMP_GROK_NAME,
                "%{MONTHNUM2}/%{MONTHDAY} %{HOUR}\\.%{MINUTE}\\.%{SECOND} in %{YEAR}"
            )
        );

        validateCustomOverrideNotMatchingBuiltInFormat(
            "'some_prefix 'dd.MM.yyyy HH:mm:ss.SSSSSS",
            "some_prefix 06.01.2018 16:56:14.295748",
            "some_prefix \\d{2}\\.\\d{2}\\.\\d{4} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}\\b",
            "CUSTOM_TIMESTAMP",
            Collections.singletonMap(
                TimestampFormatFinder.CUSTOM_TIMESTAMP_GROK_NAME,
                "some_prefix %{MONTHDAY}\\.%{MONTHNUM2}\\.%{YEAR} %{HOUR}:%{MINUTE}:%{SECOND}"
            )
        );

        validateCustomOverrideNotMatchingBuiltInFormat(
            "dd.MM. yyyy HH:mm:ss.SSSSSS",
            "06.01. 2018 16:56:14.295748",
            "\\b\\d{2}\\.\\d{2}\\. \\d{4} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}\\b",
            "CUSTOM_TIMESTAMP",
            Collections.singletonMap(
                TimestampFormatFinder.CUSTOM_TIMESTAMP_GROK_NAME,
                "%{MONTHDAY}\\.%{MONTHNUM2}\\. %{YEAR} %{HOUR}:%{MINUTE}:%{SECOND}"
            )
        );

        validateCustomOverrideNotMatchingBuiltInFormat(
            // This pattern is very close to HTTPDERROR_DATE, differing only because it contains a "d" instead of a "dd".
            // This test therefore proves that we don't decide that this override can be replaced with the built in
            // HTTPDERROR_DATE format, but do preserve it as a custom format.
            "EEE MMM d HH:mm:ss yyyy",
            "Mon Mar 7 15:03:23 2022",
            "\\b[A-Z]\\S{2} [A-Z]\\S{2} \\d{1,2} \\d{2}:\\d{2}:\\d{2} \\d{4}\\b",
            "CUSTOM_TIMESTAMP",
            Collections.singletonMap(
                TimestampFormatFinder.CUSTOM_TIMESTAMP_GROK_NAME,
                "%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE}:%{SECOND} %{YEAR}"
            )
        );
    }

    private void validateCustomOverrideNotMatchingBuiltInFormat(
        String overrideFormat,
        String text,
        String expectedSimpleRegex,
        String expectedGrokPatternName,
        Map<String, String> expectedCustomGrokPatternDefinitions
    ) {
        Consumer<Boolean> validateCustomOverrideNotMatchingBuiltInFormatGivenEcsCompatibility = (ecsCompatibility) -> {
            TimestampFormatFinder strictTimestampFormatFinder = new TimestampFormatFinder(
                explanation,
                overrideFormat,
                true,
                true,
                true,
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            strictTimestampFormatFinder.addSample(text);
            assertEquals(expectedGrokPatternName, strictTimestampFormatFinder.getGrokPatternName());
            assertEquals(expectedCustomGrokPatternDefinitions, strictTimestampFormatFinder.getCustomGrokPatternDefinitions());
            assertEquals(expectedSimpleRegex, strictTimestampFormatFinder.getSimplePattern().pattern());
            assertEquals(Collections.singletonList(overrideFormat), strictTimestampFormatFinder.getJavaTimestampFormats());
            assertEquals(1, strictTimestampFormatFinder.getNumMatchedFormats());

            TimestampFormatFinder lenientTimestampFormatFinder = new TimestampFormatFinder(
                explanation,
                overrideFormat,
                false,
                false,
                false,
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            lenientTimestampFormatFinder.addSample(text);
            lenientTimestampFormatFinder.selectBestMatch();
            assertEquals(expectedGrokPatternName, lenientTimestampFormatFinder.getGrokPatternName());
            assertEquals(expectedCustomGrokPatternDefinitions, lenientTimestampFormatFinder.getCustomGrokPatternDefinitions());
            assertEquals(expectedSimpleRegex, lenientTimestampFormatFinder.getSimplePattern().pattern());
            assertEquals(Collections.singletonList(overrideFormat), lenientTimestampFormatFinder.getJavaTimestampFormats());
            assertEquals(1, lenientTimestampFormatFinder.getNumMatchedFormats());
        };

        ecsCompatibilityModes.forEach(validateCustomOverrideNotMatchingBuiltInFormatGivenEcsCompatibility);
    }

    public void testFindFormatGivenRealLogMessages() {

        validateFindInFullMessage(
            "[2018-05-11T17:07:29,553][INFO ][o.e.e.NodeEnvironment    ] [node-0] "
                + "heap size [3.9gb], compressed ordinary object pointers [true]",
            "[",
            "TIMESTAMP_ISO8601",
            "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
            "ISO8601"
        );

        validateFindInFullMessage(
            "192.168.62.101 - - [29/Jun/2016:12:11:31 +0000] "
                + "\"POST //apiserv:8080/engine/v2/jobs HTTP/1.1\" 201 42 \"-\" \"curl/7.46.0\" 384",
            "192.168.62.101 - - [",
            "HTTPDATE",
            "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ",
            "dd/MMM/yyyy:HH:mm:ss XX"
        );

        validateFindInFullMessage(
            "Aug 29, 2009 12:03:57 AM org.apache.tomcat.util.http.Parameters processParameters",
            "",
            "CATALINA7_DATESTAMP",
            "\\b[A-Z]\\S{2} \\d{2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b",
            "MMM dd, yyyy h:mm:ss a"
        );

        validateFindInFullMessage(
            "Oct 19 17:04:44 esxi1.acme.com Vpxa: [3CB3FB90 verbose 'vpxavpxaInvtVm' "
                + "opID=WFU-33d82c31] [VpxaInvtVmChangeListener] Guest DiskInfo Changed",
            "",
            "SYSLOGTIMESTAMP",
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b",
            Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss", "MMM d HH:mm:ss")
        );

        validateFindInFullMessage(
            "559550912540598297\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t"
                + "192.168.114.28\tAuthpriv\tInfo\tsshd\tsubsystem request for sftp",
            "559550912540598297\t",
            "TIMESTAMP_ISO8601",
            "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
            "ISO8601"
        );

        validateFindInFullMessage(
            "Sep  8 11:55:35 dnsserv named[22529]: error (unexpected RCODE REFUSED) resolving " + "'www.elastic.co/A/IN': 95.110.68.206#53",
            "",
            "SYSLOGTIMESTAMP",
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b",
            Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss", "MMM d HH:mm:ss")
        );

        validateFindInFullMessage(
            "10-28-2016 16:22:47.636 +0200 ERROR Network - "
                + "Error encountered for connection from src=192.168.0.1:12345. Local side shutting down",
            "",
            "DATESTAMP",
            "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
            "MM-dd-yyyy HH:mm:ss.SSS"
        );

        validateFindInFullMessage(
            "2018-01-06 19:22:20.106822|INFO    |VirtualServer |1  |client "
                + " 'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client 'User1'(id:2) in channel '3er Instanz'(id:2)",
            "",
            "TIMESTAMP_ISO8601",
            "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
            "yyyy-MM-dd HH:mm:ss.SSSSSS"
        );

        // Differs from the above as the required format is specified
        validateFindInFullMessage(
            "yyyy-MM-dd HH:mm:ss.SSSSSS",
            "2018-01-06 19:22:20.106822|INFO    |VirtualServer |1  |client "
                + " 'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client 'User1'(id:2) in channel '3er Instanz'(id:2)",
            "",
            "TIMESTAMP_ISO8601",
            "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
            "yyyy-MM-dd HH:mm:ss.SSSSSS"
        );

        Consumer<Boolean> testFindFormatWithNonMatchingRequiredFormatGivenEcsCompatibility = (ecsCompatibility) -> {
            // Non-matching required format specified
            TimestampFormatFinder timestampFormatFinder = new TimestampFormatFinder(
                explanation,
                randomFrom("UNIX", "EEE MMM dd yyyy HH:mm zzz"),
                false,
                false,
                false,
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            timestampFormatFinder.addSample(
                "2018-01-06 19:22:20.106822|INFO    |VirtualServer |1  |client "
                    + " 'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5)"
                    + " by client 'User1'(id:2) in channel '3er Instanz'(id:2)"
            );
            assertEquals(Collections.emptyList(), timestampFormatFinder.getJavaTimestampFormats());
            assertEquals(0, timestampFormatFinder.getNumMatchedFormats());
        };

        ecsCompatibilityModes.forEach(testFindFormatWithNonMatchingRequiredFormatGivenEcsCompatibility);
    }

    public void testSelectBestMatchGivenAllSame() {
        @SuppressWarnings("checkstyle:linelength")
        String sample =
            """
                [2018-06-27T11:59:22,125][INFO ][o.e.n.Node               ] [node-0] initializing ...
                [2018-06-27T11:59:22,201][INFO ][o.e.e.NodeEnvironment    ] [node-0] using [1] data paths, mounts [[/ (/dev/disk1)]], net usable_space [216.1gb], net total_space [464.7gb], types [hfs]
                [2018-06-27T11:59:22,202][INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [494.9mb], compressed ordinary object pointers [true]
                [2018-06-27T11:59:22,204][INFO ][o.e.n.Node               ] [node-0] node name [node-0], node ID [Ha1gD8nNSDqjd6PIyu3DJA]
                [2018-06-27T11:59:22,204][INFO ][o.e.n.Node               ] [node-0] version[6.4.0-SNAPSHOT], pid[2785], build[default/zip/3c60efa/2018-06-26T14:55:15.206676Z], OS[Mac OS X/10.12.6/x86_64], JVM["Oracle Corporation"/Java HotSpot(TM) 64-Bit Server VM/10/10+46]
                [2018-06-27T11:59:22,205][INFO ][o.e.n.Node               ] [node-0] JVM arguments [-Xms1g, -Xmx1g, -XX:+UseConcMarkSweepGC, -XX:CMSInitiatingOccupancyFraction=75, -XX:+UseCMSInitiatingOccupancyOnly, -XX:+AlwaysPreTouch, -Xss1m, -Djava.awt.headless=true, -Dfile.encoding=UTF-8, -Djna.nosys=true, -XX:-OmitStackTraceInFastThrow, -Dio.netty.noUnsafe=true, -Dio.netty.noKeySetOptimization=true, -Dio.netty.recycler.maxCapacityPerThread=0, -Dlog4j.shutdownHookEnabled=false, -Dlog4j2.disable.jmx=true, -Djava.io.tmpdir=/var/folders/k5/5sqcdlps5sg3cvlp783gcz740000h0/T/elasticsearch.nFUyeMH1, -XX:+HeapDumpOnOutOfMemoryError, -XX:ErrorFile=hs_err_pid%p.log, -Xlog:gc*,gc+age=trace,safepoint:file=gc.log:utctime,level,pid,tags:filecount=32,filesize=64m, -Djava.locale.providers=COMPAT, -Dio.netty.allocator.type=unpooled, -ea, -esa, -Xms512m, -Xmx512m, -Des.path.home=/Users/dave/elasticsearch/distribution/build/cluster/run node0/elasticsearch-6.4.0-SNAPSHOT, -Des.path.conf=/Users/dave/elasticsearch/distribution/build/cluster/run node0/elasticsearch-6.4.0-SNAPSHOT/config, -Des.distribution.flavor=default, -Des.distribution.type=zip]
                [2018-06-27T11:59:22,205][WARN ][o.e.n.Node               ] [node-0] version [6.4.0-SNAPSHOT] is a pre-release version of Elasticsearch and is not suitable for production
                [2018-06-27T11:59:23,585][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [aggs-matrix-stats]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [analysis-common]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [ingest-common]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [lang-expression]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [lang-mustache]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [lang-painless]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [mapper-extras]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [parent-join]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [percolator]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [rank-eval]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [reindex]
                [2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [repository-url]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [transport-netty4]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-core]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-deprecation]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-graph]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-logstash]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-ml]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-monitoring]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-rollup]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-security]
                [2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-sql]
                [2018-06-27T11:59:23,588][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-upgrade]
                [2018-06-27T11:59:23,588][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-watcher]
                [2018-06-27T11:59:23,588][INFO ][o.e.p.PluginsService     ] [node-0] no plugins loaded
                """;

        TimestampFormatFinder timestampFormatFinder = LogTextStructureFinder.populateTimestampFormatFinder(
            explanation,
            sample.split("\n"),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        timestampFormatFinder.selectBestMatch();
        assertEquals(Collections.singletonList("ISO8601"), timestampFormatFinder.getJavaTimestampFormats());
        assertEquals("TIMESTAMP_ISO8601", timestampFormatFinder.getGrokPatternName());
        assertEquals("\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}", timestampFormatFinder.getSimplePattern().pattern());
        for (String preface : timestampFormatFinder.getPrefaces()) {
            assertEquals("[", preface);
        }
        assertEquals(1, timestampFormatFinder.getNumMatchedFormats());
    }

    public void testSelectBestMatchGivenExceptionTrace() {

        TimestampFormatFinder timestampFormatFinder = LogTextStructureFinder.populateTimestampFormatFinder(
            explanation,
            EXCEPTION_TRACE_SAMPLE.split("\n"),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );

        // Even though many lines have a timestamp near the end (in the Lucene version information),
        // these are so far along the lines that the weight of the timestamp near the beginning of the
        // first line should take precedence
        timestampFormatFinder.selectBestMatch();
        assertEquals(Collections.singletonList("ISO8601"), timestampFormatFinder.getJavaTimestampFormats());
        assertEquals("TIMESTAMP_ISO8601", timestampFormatFinder.getGrokPatternName());
        assertEquals("\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}", timestampFormatFinder.getSimplePattern().pattern());
        for (String preface : timestampFormatFinder.getPrefaces()) {
            assertEquals("[", preface);
        }
        assertEquals(2, timestampFormatFinder.getNumMatchedFormats());
    }

    public void testSelectBestMatchGivenExceptionTraceAndTimestampFormatOverride() {

        TextStructureOverrides overrides = TextStructureOverrides.builder().setTimestampFormat("yyyy-MM-dd HH:mm:ss").build();

        TimestampFormatFinder timestampFormatFinder = LogTextStructureFinder.populateTimestampFormatFinder(
            explanation,
            EXCEPTION_TRACE_SAMPLE.split("\n"),
            overrides,
            NOOP_TIMEOUT_CHECKER
        );

        // The override should force the seemingly inferior choice of timestamp
        // TODO - this won't work any more :-(
    }

    public void testSelectBestMatchGivenExceptionTraceAndImpossibleTimestampFormatOverride() {

        TextStructureOverrides overrides = TextStructureOverrides.builder().setTimestampFormat("MMM dd HH:mm:ss").build();

        TimestampFormatFinder timestampFormatFinder = LogTextStructureFinder.populateTimestampFormatFinder(
            explanation,
            EXCEPTION_TRACE_SAMPLE.split("\n"),
            overrides,
            NOOP_TIMEOUT_CHECKER
        );

        timestampFormatFinder.selectBestMatch();
        assertEquals(Collections.emptyList(), timestampFormatFinder.getJavaTimestampFormats());
        assertNull(timestampFormatFinder.getGrokPatternName());
        assertNull(timestampFormatFinder.getSimplePattern());
        assertEquals(Collections.emptyList(), timestampFormatFinder.getPrefaces());
        assertEquals(0, timestampFormatFinder.getNumMatchedFormats());
    }

    private void validateNoTimestampMatch(String text) {

        TimestampFormatFinder strictTimestampFormatFinder = new TimestampFormatFinder(explanation, true, true, true, NOOP_TIMEOUT_CHECKER);
        expectThrows(IllegalArgumentException.class, () -> strictTimestampFormatFinder.addSample(text));
        assertEquals(0, strictTimestampFormatFinder.getNumMatchedFormats());

        TimestampFormatFinder lenientTimestampFormatFinder = new TimestampFormatFinder(
            explanation,
            false,
            false,
            false,
            NOOP_TIMEOUT_CHECKER
        );
        lenientTimestampFormatFinder.addSample(text);
        lenientTimestampFormatFinder.selectBestMatch();
        assertNull(lenientTimestampFormatFinder.getGrokPatternName());
        assertEquals(0, lenientTimestampFormatFinder.getNumMatchedFormats());
    }

    private void validateTimestampMatch(
        String text,
        String expectedGrokPatternName,
        String expectedSimpleRegex,
        String expectedJavaTimestampFormat,
        long expectedEpochMs,
        boolean ecsCompatibility
    ) {
        validateTimestampMatch(
            text,
            expectedGrokPatternName,
            expectedSimpleRegex,
            Collections.singletonList(expectedJavaTimestampFormat),
            expectedEpochMs,
            ecsCompatibility
        );
    }

    private void validateTimestampMatch(
        String text,
        String expectedGrokPatternName,
        String expectedSimpleRegex,
        List<String> expectedJavaTimestampFormats,
        long expectedEpochMs,
        boolean ecsCompatibility
    ) {

        Pattern expectedSimplePattern = Pattern.compile(expectedSimpleRegex);
        assertTrue(expectedSimplePattern.matcher(text).find());
        validateJavaTimestampFormats(expectedJavaTimestampFormats, text, expectedEpochMs);

        TimestampFormatFinder strictTimestampFormatFinder = new TimestampFormatFinder(
            explanation,
            true,
            true,
            true,
            NOOP_TIMEOUT_CHECKER,
            ecsCompatibility
        );
        strictTimestampFormatFinder.addSample(text);
        assertEquals(expectedSimplePattern.pattern(), strictTimestampFormatFinder.getSimplePattern().pattern());
        assertEquals(expectedJavaTimestampFormats, strictTimestampFormatFinder.getJavaTimestampFormats());
        assertEquals(1, strictTimestampFormatFinder.getNumMatchedFormats());

        TimestampFormatFinder lenientTimestampFormatFinder = new TimestampFormatFinder(
            explanation,
            false,
            false,
            false,
            NOOP_TIMEOUT_CHECKER,
            ecsCompatibility
        );
        lenientTimestampFormatFinder.addSample(text);
        lenientTimestampFormatFinder.selectBestMatch();
        assertEquals(expectedSimplePattern.pattern(), lenientTimestampFormatFinder.getSimplePattern().pattern());
        assertEquals(expectedJavaTimestampFormats, lenientTimestampFormatFinder.getJavaTimestampFormats());
        assertEquals(1, lenientTimestampFormatFinder.getNumMatchedFormats());

        if (ecsCompatibility) {
            if ("TOMCAT_DATESTAMP".equals(expectedGrokPatternName)) {
                assertEquals("TOMCATLEGACY_DATESTAMP", strictTimestampFormatFinder.getGrokPatternName());
                assertEquals("TOMCATLEGACY_DATESTAMP", lenientTimestampFormatFinder.getGrokPatternName());
            } else if ("CATALINA_DATESTAMP".equals(expectedGrokPatternName)) {
                assertEquals("CATALINA7_DATESTAMP", strictTimestampFormatFinder.getGrokPatternName());
                assertEquals("CATALINA7_DATESTAMP", lenientTimestampFormatFinder.getGrokPatternName());
            }
        } else {
            assertEquals(expectedGrokPatternName, strictTimestampFormatFinder.getGrokPatternName());
            assertEquals(expectedGrokPatternName, lenientTimestampFormatFinder.getGrokPatternName());
        }
    }

    private void validateFindInFullMessage(
        String message,
        String expectedPreface,
        String expectedGrokPatternName,
        String expectedSimpleRegex,
        String expectedJavaTimestampFormat
    ) {
        validateFindInFullMessage(
            message,
            expectedPreface,
            expectedGrokPatternName,
            expectedSimpleRegex,
            Collections.singletonList(expectedJavaTimestampFormat)
        );
    }

    private void validateFindInFullMessage(
        String timestampFormatOverride,
        String message,
        String expectedPreface,
        String expectedGrokPatternName,
        String expectedSimpleRegex,
        String expectedJavaTimestampFormat
    ) {
        validateFindInFullMessage(
            timestampFormatOverride,
            message,
            expectedPreface,
            expectedGrokPatternName,
            expectedSimpleRegex,
            Collections.singletonList(expectedJavaTimestampFormat)
        );
    }

    private void validateFindInFullMessage(
        String message,
        String expectedPreface,
        String expectedGrokPatternName,
        String expectedSimpleRegex,
        List<String> expectedJavaTimestampFormats
    ) {
        validateFindInFullMessage(
            null,
            message,
            expectedPreface,
            expectedGrokPatternName,
            expectedSimpleRegex,
            expectedJavaTimestampFormats
        );
    }

    private void validateFindInFullMessage(
        String timestampFormatOverride,
        String message,
        String expectedPreface,
        String expectedGrokPatternName,
        String expectedSimpleRegex,
        List<String> expectedJavaTimestampFormats
    ) {

        Consumer<Boolean> validateFindInFullMessageGivenEcsCompatibility = (ecsCompatibility) -> {
            Pattern expectedSimplePattern = Pattern.compile(expectedSimpleRegex);
            assertTrue(expectedSimplePattern.matcher(message).find());

            TimestampFormatFinder timestampFormatFinder = new TimestampFormatFinder(
                explanation,
                timestampFormatOverride,
                false,
                false,
                false,
                NOOP_TIMEOUT_CHECKER,
                ecsCompatibility
            );
            timestampFormatFinder.addSample(message);
            timestampFormatFinder.selectBestMatch();
            if ("CATALINA7_DATESTAMP".equals(expectedGrokPatternName)) {
                if (ecsCompatibility) {
                    assertEquals(expectedGrokPatternName, timestampFormatFinder.getGrokPatternName());
                } else {
                    assertEquals("CATALINA_DATESTAMP", timestampFormatFinder.getGrokPatternName());
                }
            }
            assertEquals(expectedSimplePattern.pattern(), timestampFormatFinder.getSimplePattern().pattern());
            assertEquals(expectedJavaTimestampFormats, timestampFormatFinder.getJavaTimestampFormats());
            assertEquals(Collections.singletonList(expectedPreface), timestampFormatFinder.getPrefaces());
            assertEquals(1, timestampFormatFinder.getNumMatchedFormats());
        };

        ecsCompatibilityModes.forEach(validateFindInFullMessageGivenEcsCompatibility);
    }

    private void validateJavaTimestampFormats(List<String> javaTimestampFormats, String text, long expectedEpochMs) {

        // All the test times are for Tue May 15 2018 16:14:56 UTC, which is 17:14:56 in London.
        // This is the timezone that will be used for any text representations that don't include it.
        ZoneId defaultZone = ZoneId.of("Europe/London");
        long actualEpochMs;
        for (int i = 0; i < javaTimestampFormats.size(); ++i) {
            try {
                String timestampFormat = javaTimestampFormats.get(i);
                switch (timestampFormat) {
                    case "ISO8601" -> actualEpochMs = DateFormatter.forPattern("iso8601").withZone(defaultZone).parseMillis(text);
                    case "UNIX_MS" -> actualEpochMs = Long.parseLong(text);
                    case "UNIX" -> actualEpochMs = (long) (Double.parseDouble(text) * 1000.0);
                    case "TAI64N" -> actualEpochMs = parseMillisFromTai64n(text);
                    default -> {
                        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().appendPattern(timestampFormat);
                        if (timestampFormat.indexOf('y') == -1) {
                            builder.parseDefaulting(ChronoField.YEAR_OF_ERA, 2018);
                        }
                        if (timestampFormat.indexOf('m') == -1) {
                            // All formats tested have either both or neither of hour and minute
                            builder.parseDefaulting(ChronoField.HOUR_OF_DAY, 0);
                            builder.parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0);
                            // Seconds automatically defaults to 0
                        }
                        DateTimeFormatter parser = builder.toFormatter(Locale.ROOT);
                        // This next line parses the textual date without any default timezone, so if
                        // the text doesn't contain the timezone then the resulting temporal accessor
                        // will be incomplete (i.e. impossible to convert to an Instant). You would
                        // hope that it would be possible to specify a timezone to be used only in this
                        // case, and in Java 9 and 10 it is, by adding withZone(zone) before the
                        // parse(text) call. However, with Java 8 this overrides any timezone parsed
                        // from the text. The solution is to parse twice, once without a default
                        // timezone and then again with a default timezone if the first parse didn't
                        // find one in the text.
                        TemporalAccessor parsed = parser.parse(text);
                        if (parsed.query(TemporalQueries.zone()) == null) {
                            // TODO: when Java 8 is no longer supported remove the two
                            // lines and comment above and the closing brace below
                            parsed = parser.withZone(defaultZone).parse(text);
                        }
                        actualEpochMs = Instant.from(parsed).toEpochMilli();
                    }
                }
                if (expectedEpochMs == actualEpochMs) {
                    break;
                }
                // If the last one isn't right then propagate
                if (i == javaTimestampFormats.size() - 1) {
                    assertEquals(expectedEpochMs, actualEpochMs);
                }
            } catch (RuntimeException e) {
                // If the last one throws then propagate
                if (i == javaTimestampFormats.size() - 1) {
                    throw e;
                }
            }
        }
    }

    /**
     * Logic copied from {@code org.elasticsearch.ingest.common.DateFormat.Tai64n.parseMillis}.
     */
    private long parseMillisFromTai64n(String tai64nDate) {
        if (tai64nDate.startsWith("@")) {
            tai64nDate = tai64nDate.substring(1);
        }
        assertEquals(24, tai64nDate.length());
        long seconds = Long.parseLong(tai64nDate.substring(1, 16), 16);
        long nanos = Long.parseLong(tai64nDate.substring(16, 24), 16);
        return (seconds * 1000) - 10000 + nanos / 1000000;
    }
}
