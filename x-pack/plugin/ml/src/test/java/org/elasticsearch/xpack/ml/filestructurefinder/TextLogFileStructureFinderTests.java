/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FieldStats;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;
import org.elasticsearch.xpack.ml.filestructurefinder.TimestampFormatFinder.TimestampMatch;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class TextLogFileStructureFinderTests extends FileStructureTestCase {

    private static final String EXCEPTION_TRACE_SAMPLE =
        "[2018-02-28T14:49:40,517][DEBUG][o.e.a.b.TransportShardBulkAction] [an_index][2] failed to execute bulk item " +
            "(index) BulkShardRequest [[an_index][2]] containing [33] requests\n" +
        "java.lang.IllegalArgumentException: Document contains at least one immense term in field=\"message.keyword\" (whose UTF8 " +
            "encoding is longer than the max length 32766), all of which were skipped.  Please correct the analyzer to not produce " +
            "such terms.  The prefix of the first immense term is: '[60, 83, 79, 65, 80, 45, 69, 78, 86, 58, 69, 110, 118, 101, 108, " +
            "111, 112, 101, 32, 120, 109, 108, 110, 115, 58, 83, 79, 65, 80, 45]...', original message: bytes can be at most 32766 " +
            "in length; got 49023\n" +
        "\tat org.apache.lucene.index.DefaultIndexingChain$PerField.invert(DefaultIndexingChain.java:796) " +
            "~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]\n" +
        "\tat org.apache.lucene.index.DefaultIndexingChain.processField(DefaultIndexingChain.java:430) " +
            "~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]\n" +
        "\tat org.apache.lucene.index.DefaultIndexingChain.processDocument(DefaultIndexingChain.java:392) " +
            "~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]\n" +
        "\tat org.apache.lucene.index.DocumentsWriterPerThread.updateDocument(DocumentsWriterPerThread.java:240) " +
            "~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]\n" +
        "\tat org.apache.lucene.index.DocumentsWriter.updateDocument(DocumentsWriter.java:496) " +
            "~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]\n" +
        "\tat org.apache.lucene.index.IndexWriter.updateDocument(IndexWriter.java:1729) " +
            "~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]\n" +
        "\tat org.apache.lucene.index.IndexWriter.addDocument(IndexWriter.java:1464) " +
            "~[lucene-core-7.2.1.jar:7.2.1 b2b6438b37073bee1fca40374e85bf91aa457c0b - ubuntu - 2018-01-10 00:48:43]\n" +
        "\tat org.elasticsearch.index.engine.InternalEngine.index(InternalEngine.java:1070) ~[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.index.engine.InternalEngine.indexIntoLucene(InternalEngine.java:1012) " +
            "~[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.index.engine.InternalEngine.index(InternalEngine.java:878) ~[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.index.shard.IndexShard.index(IndexShard.java:738) ~[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.index.shard.IndexShard.applyIndexOperation(IndexShard.java:707) ~[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.index.shard.IndexShard.applyIndexOperationOnPrimary(IndexShard.java:673) " +
            "~[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.bulk.TransportShardBulkAction.executeIndexRequestOnPrimary(TransportShardBulkAction.java:548) " +
            "~[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.bulk.TransportShardBulkAction.executeIndexRequest(TransportShardBulkAction.java:140) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.bulk.TransportShardBulkAction.executeBulkItemRequest(TransportShardBulkAction.java:236) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.bulk.TransportShardBulkAction.performOnPrimary(TransportShardBulkAction.java:123) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.bulk.TransportShardBulkAction.shardOperationOnPrimary(TransportShardBulkAction.java:110) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.bulk.TransportShardBulkAction.shardOperationOnPrimary(TransportShardBulkAction.java:72) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryShardReference.perform" +
            "(TransportReplicationAction.java:1034) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryShardReference.perform" +
            "(TransportReplicationAction.java:1012) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.ReplicationOperation.execute(ReplicationOperation.java:103) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$AsyncPrimaryAction.onResponse" +
            "(TransportReplicationAction.java:359) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$AsyncPrimaryAction.onResponse" +
            "(TransportReplicationAction.java:299) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$1.onResponse" +
            "(TransportReplicationAction.java:975) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$1.onResponse" +
            "(TransportReplicationAction.java:972) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.index.shard.IndexShardOperationPermits.acquire(IndexShardOperationPermits.java:238) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.index.shard.IndexShard.acquirePrimaryOperationPermit(IndexShard.java:2220) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction.acquirePrimaryShardReference" +
            "(TransportReplicationAction.java:984) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction.access$500(TransportReplicationAction.java:98) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$AsyncPrimaryAction.doRun" +
            "(TransportReplicationAction.java:320) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:37) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryOperationTransportHandler" +
            ".messageReceived(TransportReplicationAction.java:295) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryOperationTransportHandler" +
            ".messageReceived(TransportReplicationAction.java:282) [elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.transport.RequestHandlerRegistry.processMessageReceived(RequestHandlerRegistry.java:66) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.transport.TransportService$7.doRun(TransportService.java:656) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.common.util.concurrent.ThreadContext$ContextPreservingAbstractRunnable.doRun(ThreadContext.java:635) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:37) " +
            "[elasticsearch-6.2.1.jar:6.2.1]\n" +
        "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_144]\n" +
        "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_144]\n" +
        "\tat java.lang.Thread.run(Thread.java:748) [?:1.8.0_144]\n";

    private FileStructureFinderFactory factory = new TextLogFileStructureFinderFactory();

    public void testCreateConfigsGivenElasticsearchLog() throws Exception {
        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        FileStructureFinder structureFinder = factory.createFromSample(explanation, TEXT_SAMPLE, charset, hasByteOrderMarker,
            FileStructureOverrides.EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);

        FileStructure structure = structureFinder.getStructure();

        assertEquals(FileStructure.Format.SEMI_STRUCTURED_TEXT, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\[\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertEquals("\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} \\]\\[.*", structure.getGrokPattern());
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("ISO8601"), structure.getJodaTimestampFormats());
        FieldStats messageFieldStats = structure.getFieldStats().get("message");
        assertNotNull(messageFieldStats);
        for (String statMessage : messageFieldStats.getTopHits().stream().map(m -> (String) m.get("value")).collect(Collectors.toList())) {
            assertThat(structureFinder.getSampleMessages(), hasItem(statMessage));
        }
    }

    public void testCreateConfigsGivenElasticsearchLogAndTimestampFieldOverride() throws Exception {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setTimestampField("my_time").build();

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        FileStructureFinder structureFinder = factory.createFromSample(explanation, TEXT_SAMPLE, charset, hasByteOrderMarker, overrides,
            NOOP_TIMEOUT_CHECKER);

        FileStructure structure = structureFinder.getStructure();

        assertEquals(FileStructure.Format.SEMI_STRUCTURED_TEXT, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\[\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertEquals("\\[%{TIMESTAMP_ISO8601:my_time}\\]\\[%{LOGLEVEL:loglevel} \\]\\[.*", structure.getGrokPattern());
        assertEquals("my_time", structure.getTimestampField());
        assertEquals(Collections.singletonList("ISO8601"), structure.getJodaTimestampFormats());
        FieldStats messageFieldStats = structure.getFieldStats().get("message");
        assertNotNull(messageFieldStats);
        for (String statMessage : messageFieldStats.getTopHits().stream().map(m -> (String) m.get("value")).collect(Collectors.toList())) {
            assertThat(structureFinder.getSampleMessages(), hasItem(statMessage));
        }
    }

    public void testCreateConfigsGivenElasticsearchLogAndGrokPatternOverride() throws Exception {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setGrokPattern("\\[%{TIMESTAMP_ISO8601:timestamp}\\]" +
            "\\[%{LOGLEVEL:loglevel} *\\]\\[%{JAVACLASS:class} *\\] \\[%{HOSTNAME:node}\\] %{JAVALOGMESSAGE:message}").build();

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        FileStructureFinder structureFinder = factory.createFromSample(explanation, TEXT_SAMPLE, charset, hasByteOrderMarker, overrides,
            NOOP_TIMEOUT_CHECKER);

        FileStructure structure = structureFinder.getStructure();

        assertEquals(FileStructure.Format.SEMI_STRUCTURED_TEXT, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\[\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertEquals("\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} *\\]" +
            "\\[%{JAVACLASS:class} *\\] \\[%{HOSTNAME:node}\\] %{JAVALOGMESSAGE:message}", structure.getGrokPattern());
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("ISO8601"), structure.getJodaTimestampFormats());
        FieldStats messageFieldStats = structure.getFieldStats().get("message");
        assertNotNull(messageFieldStats);
        for (String statMessage : messageFieldStats.getTopHits().stream().map(m -> (String) m.get("value")).collect(Collectors.toList())) {
            // In this case the "message" field was output by the Grok pattern, so "message"
            // at the end of the processing will _not_ contain a complete sample message
            assertThat(structureFinder.getSampleMessages(), not(hasItem(statMessage)));
        }
    }

    public void testCreateConfigsGivenElasticsearchLogAndImpossibleGrokPatternOverride() {

        // This Grok pattern cannot be matched against the messages in the sample because the fields are in the wrong order
        FileStructureOverrides overrides = FileStructureOverrides.builder().setGrokPattern("\\[%{LOGLEVEL:loglevel} *\\]" +
            "\\[%{HOSTNAME:node}\\]\\[%{TIMESTAMP_ISO8601:timestamp}\\] \\[%{JAVACLASS:class} *\\] %{JAVALOGMESSAGE:message}").build();

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> factory.createFromSample(explanation, TEXT_SAMPLE, charset, hasByteOrderMarker, overrides, NOOP_TIMEOUT_CHECKER));

        assertEquals("Supplied Grok pattern [\\[%{LOGLEVEL:loglevel} *\\]\\[%{HOSTNAME:node}\\]\\[%{TIMESTAMP_ISO8601:timestamp}\\] " +
            "\\[%{JAVACLASS:class} *\\] %{JAVALOGMESSAGE:message}] does not match sample messages", e.getMessage());
    }

    public void testCreateMultiLineMessageStartRegexGivenNoPrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^" + simpleDateRegex.replaceFirst("^\\\\b", ""),
                TextLogFileStructureFinder.createMultiLineMessageStartRegex(Collections.emptySet(), simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenOneEmptyPreface() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^" + simpleDateRegex.replaceFirst("^\\\\b", ""),
                TextLogFileStructureFinder.createMultiLineMessageStartRegex(Collections.singleton(""), simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenOneLogLevelPreface() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^\\[.*?\\] \\[" + simpleDateRegex,
                TextLogFileStructureFinder.createMultiLineMessageStartRegex(Collections.singleton("[ERROR] ["), simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyLogLevelPrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("[ERROR] [", "[DEBUG] [");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^\\[.*?\\] \\[" + simpleDateRegex,
                TextLogFileStructureFinder.createMultiLineMessageStartRegex(prefaces, simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyHostnamePrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("host-1.acme.com|", "my_host.elastic.co|");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^.*?\\|" + simpleDateRegex,
                TextLogFileStructureFinder.createMultiLineMessageStartRegex(prefaces, simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyPrefacesIncludingEmpty() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("", "[non-standard] ");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^.*?" + simpleDateRegex,
                TextLogFileStructureFinder.createMultiLineMessageStartRegex(prefaces, simpleDateRegex));
        }
    }

    public void testMostLikelyTimestampGivenAllSame() {
        String sample = "[2018-06-27T11:59:22,125][INFO ][o.e.n.Node               ] [node-0] initializing ...\n" +
            "[2018-06-27T11:59:22,201][INFO ][o.e.e.NodeEnvironment    ] [node-0] using [1] data paths, mounts [[/ (/dev/disk1)]], " +
                "net usable_space [216.1gb], net total_space [464.7gb], types [hfs]\n" +
            "[2018-06-27T11:59:22,202][INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [494.9mb], " +
                "compressed ordinary object pointers [true]\n" +
            "[2018-06-27T11:59:22,204][INFO ][o.e.n.Node               ] [node-0] node name [node-0], node ID [Ha1gD8nNSDqjd6PIyu3DJA]\n" +
            "[2018-06-27T11:59:22,204][INFO ][o.e.n.Node               ] [node-0] version[6.4.0-SNAPSHOT], pid[2785], " +
                "build[default/zip/3c60efa/2018-06-26T14:55:15.206676Z], OS[Mac OS X/10.12.6/x86_64], " +
                "JVM[\"Oracle Corporation\"/Java HotSpot(TM) 64-Bit Server VM/10/10+46]\n" +
            "[2018-06-27T11:59:22,205][INFO ][o.e.n.Node               ] [node-0] JVM arguments [-Xms1g, -Xmx1g, " +
                "-XX:+UseConcMarkSweepGC, -XX:CMSInitiatingOccupancyFraction=75, -XX:+UseCMSInitiatingOccupancyOnly, " +
                "-XX:+AlwaysPreTouch, -Xss1m, -Djava.awt.headless=true, -Dfile.encoding=UTF-8, -Djna.nosys=true, " +
                "-XX:-OmitStackTraceInFastThrow, -Dio.netty.noUnsafe=true, -Dio.netty.noKeySetOptimization=true, " +
                "-Dio.netty.recycler.maxCapacityPerThread=0, -Dlog4j.shutdownHookEnabled=false, -Dlog4j2.disable.jmx=true, " +
                "-Djava.io.tmpdir=/var/folders/k5/5sqcdlps5sg3cvlp783gcz740000h0/T/elasticsearch.nFUyeMH1, " +
                "-XX:+HeapDumpOnOutOfMemoryError, -XX:HeapDumpPath=data, -XX:ErrorFile=logs/hs_err_pid%p.log, " +
                "-Xlog:gc*,gc+age=trace,safepoint:file=logs/gc.log:utctime,pid,tags:filecount=32,filesize=64m, " +
                "-Djava.locale.providers=COMPAT, -Dio.netty.allocator.type=unpooled, -ea, -esa, -Xms512m, -Xmx512m, " +
                "-Des.path.home=/Users/dave/elasticsearch/distribution/build/cluster/run node0/elasticsearch-6.4.0-SNAPSHOT, " +
                "-Des.path.conf=/Users/dave/elasticsearch/distribution/build/cluster/run node0/elasticsearch-6.4.0-SNAPSHOT/config, " +
                "-Des.distribution.flavor=default, -Des.distribution.type=zip]\n" +
            "[2018-06-27T11:59:22,205][WARN ][o.e.n.Node               ] [node-0] version [6.4.0-SNAPSHOT] is a pre-release version of " +
                "Elasticsearch and is not suitable for production\n" +
            "[2018-06-27T11:59:23,585][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [aggs-matrix-stats]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [analysis-common]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [ingest-common]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [lang-expression]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [lang-mustache]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [lang-painless]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [mapper-extras]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [parent-join]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [percolator]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [rank-eval]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [reindex]\n" +
            "[2018-06-27T11:59:23,586][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [repository-url]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [transport-netty4]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-core]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-deprecation]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-graph]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-logstash]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-ml]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-monitoring]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-rollup]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-security]\n" +
            "[2018-06-27T11:59:23,587][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-sql]\n" +
            "[2018-06-27T11:59:23,588][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-upgrade]\n" +
            "[2018-06-27T11:59:23,588][INFO ][o.e.p.PluginsService     ] [node-0] loaded module [x-pack-watcher]\n" +
            "[2018-06-27T11:59:23,588][INFO ][o.e.p.PluginsService     ] [node-0] no plugins loaded\n";

        Tuple<TimestampMatch, Set<String>> mostLikelyMatch =
            TextLogFileStructureFinder.mostLikelyTimestamp(sample.split("\n"), FileStructureOverrides.EMPTY_OVERRIDES,
                NOOP_TIMEOUT_CHECKER);
        assertNotNull(mostLikelyMatch);
        assertEquals(new TimestampMatch(9, "", "ISO8601", "yyyy-MM-dd'T'HH:mm:ss,SSS",
            "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), mostLikelyMatch.v1());
    }

    public void testMostLikelyTimestampGivenExceptionTrace() {

        Tuple<TimestampMatch, Set<String>> mostLikelyMatch =
            TextLogFileStructureFinder.mostLikelyTimestamp(EXCEPTION_TRACE_SAMPLE.split("\n"), FileStructureOverrides.EMPTY_OVERRIDES,
                NOOP_TIMEOUT_CHECKER);
        assertNotNull(mostLikelyMatch);

        // Even though many lines have a timestamp near the end (in the Lucene version information),
        // these are so far along the lines that the weight of the timestamp near the beginning of the
        // first line should take precedence
        assertEquals(new TimestampMatch(9, "", "ISO8601", "yyyy-MM-dd'T'HH:mm:ss,SSS",
            "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), mostLikelyMatch.v1());
    }

    public void testMostLikelyTimestampGivenExceptionTraceAndTimestampFormatOverride() {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setTimestampFormat("YYYY-MM-dd HH:mm:ss").build();

        Tuple<TimestampMatch, Set<String>> mostLikelyMatch =
            TextLogFileStructureFinder.mostLikelyTimestamp(EXCEPTION_TRACE_SAMPLE.split("\n"), overrides, NOOP_TIMEOUT_CHECKER);
        assertNotNull(mostLikelyMatch);

        // The override should force the seemingly inferior choice of timestamp
        assertEquals(new TimestampMatch(6, "", "YYYY-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
            "TIMESTAMP_ISO8601", ""), mostLikelyMatch.v1());
    }

    public void testMostLikelyTimestampGivenExceptionTraceAndImpossibleTimestampFormatOverride() {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setTimestampFormat("MMM dd HH:mm:ss").build();

        Tuple<TimestampMatch, Set<String>> mostLikelyMatch =
            TextLogFileStructureFinder.mostLikelyTimestamp(EXCEPTION_TRACE_SAMPLE.split("\n"), overrides, NOOP_TIMEOUT_CHECKER);
        assertNull(mostLikelyMatch);
    }
}
