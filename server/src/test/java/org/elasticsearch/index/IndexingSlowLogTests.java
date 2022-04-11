/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.Term;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.MockAppender;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingSlowLog.IndexingSlowLogMessage;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class IndexingSlowLogTests extends ESTestCase {
    static MockAppender appender;
    static Logger testLogger1 = LogManager.getLogger(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_PREFIX + ".index");

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(testLogger1, appender);
    }

    @AfterClass
    public static void cleanup() {
        appender.stop();
        Loggers.removeAppender(testLogger1, appender);
    }

    public void testLevelPrecedence() {
        String uuid = UUIDs.randomBase64UUID();
        IndexMetadata metadata = createIndexMetadata("index-precedence", settings(uuid));
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        IndexingSlowLog log = new IndexingSlowLog(settings);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", EngineTestCase.randomIdFieldType(), null);
        Engine.Index index = new Engine.Index(new Term("_id", Uid.encodeId("doc_id")), randomNonNegativeLong(), doc);
        Engine.IndexResult result = Mockito.mock(Engine.IndexResult.class);// (0, 0, SequenceNumbers.UNASSIGNED_SEQ_NO, false);
        Mockito.when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);

        // For this test, when level is not breached, the level below should be used.
        {
            Mockito.when(result.getTook()).thenReturn(40L);
            log.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertThat(appender.getLastEventAndReset().getLevel(), equalTo(Level.INFO));

            Mockito.when(result.getTook()).thenReturn(41L);
            log.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertThat(appender.getLastEventAndReset().getLevel(), equalTo(Level.WARN));
        }

        {
            Mockito.when(result.getTook()).thenReturn(30L);
            log.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertThat(appender.getLastEventAndReset().getLevel(), equalTo(Level.DEBUG));

            Mockito.when(result.getTook()).thenReturn(31L);
            log.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertThat(appender.getLastEventAndReset().getLevel(), equalTo(Level.INFO));
        }

        {
            Mockito.when(result.getTook()).thenReturn(20L);
            log.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertThat(appender.getLastEventAndReset().getLevel(), equalTo(Level.TRACE));

            Mockito.when(result.getTook()).thenReturn(21L);
            log.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertThat(appender.getLastEventAndReset().getLevel(), equalTo(Level.DEBUG));
        }

        {
            Mockito.when(result.getTook()).thenReturn(10L);
            log.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertNull(appender.getLastEventAndReset());

            Mockito.when(result.getTook()).thenReturn(11L);
            log.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertThat(appender.getLastEventAndReset().getLevel(), equalTo(Level.TRACE));
        }
    }

    public void testTwoLoggersDifferentLevel() {
        IndexSettings index1Settings = new IndexSettings(
            createIndexMetadata(
                "index1",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING.getKey(), "40nanos")
                    .build()
            ),
            Settings.EMPTY
        );
        IndexingSlowLog log1 = new IndexingSlowLog(index1Settings);

        IndexSettings index2Settings = new IndexSettings(
            createIndexMetadata(
                "index2",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING.getKey(), "10nanos")
                    .build()
            ),
            Settings.EMPTY
        );
        IndexingSlowLog log2 = new IndexingSlowLog(index2Settings);

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", EngineTestCase.randomIdFieldType(), null);
        Engine.Index index = new Engine.Index(new Term("_id", Uid.encodeId("doc_id")), randomNonNegativeLong(), doc);
        Engine.IndexResult result = Mockito.mock(Engine.IndexResult.class);
        Mockito.when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);

        {
            Mockito.when(result.getTook()).thenReturn(11L);

            // threshold set on WARN(40nanos) where 11nanos does not breach, should not log
            log1.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertNull(appender.getLastEventAndReset());

            // threshold set on TRACE(10nanos) and 11nanos breaches it, should log
            log2.postIndex(ShardId.fromString("[index][123]"), index, result);
            assertNotNull(appender.getLastEventAndReset());
        }
    }

    public void testMultipleSlowLoggersUseSingleLog4jLogger() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);

        IndexSettings index1Settings = new IndexSettings(createIndexMetadata("index1", settings(UUIDs.randomBase64UUID())), Settings.EMPTY);
        IndexingSlowLog log1 = new IndexingSlowLog(index1Settings);

        int numberOfLoggersBefore = context.getLoggers().size();

        IndexSettings index2Settings = new IndexSettings(createIndexMetadata("index2", settings(UUIDs.randomBase64UUID())), Settings.EMPTY);
        IndexingSlowLog log2 = new IndexingSlowLog(index2Settings);
        context = (LoggerContext) LogManager.getContext(false);

        int numberOfLoggersAfter = context.getLoggers().size();
        assertThat(numberOfLoggersAfter, equalTo(numberOfLoggersBefore));
    }

    private IndexMetadata createIndexMetadata(String index, Settings build) {
        return newIndexMeta(index, build);
    }

    private Settings settings(String uuid) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING.getKey(), "10nanos")
            .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING.getKey(), "20nanos")
            .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING.getKey(), "30nanos")
            .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING.getKey(), "40nanos")
            .build();
    }

    public void testSlowLogMessageHasJsonFields() throws IOException {
        BytesReference source = BytesReference.bytes(JsonXContent.contentBuilder().startObject().field("foo", "bar").endObject());
        ParsedDocument pd = new ParsedDocument(
            new NumericDocValuesField("version", 1),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            "id",
            "routingValue",
            null,
            source,
            XContentType.JSON,
            null
        );
        Index index = new Index("foo", "123");
        // Turning off document logging doesn't log source[]
        ESLogMessage p = IndexingSlowLogMessage.of(index, pd, 10, true, 0);

        assertThat(p.get("elasticsearch.slowlog.message"), equalTo("[foo/123]"));
        assertThat(p.get("elasticsearch.slowlog.took"), equalTo("10nanos"));
        assertThat(p.get("elasticsearch.slowlog.took_millis"), equalTo("0"));
        assertThat(p.get("elasticsearch.slowlog.id"), equalTo("id"));
        assertThat(p.get("elasticsearch.slowlog.routing"), equalTo("routingValue"));
        assertThat(p.get("elasticsearch.slowlog.source"), is(emptyOrNullString()));

        // Turning on document logging logs the whole thing
        p = IndexingSlowLogMessage.of(index, pd, 10, true, Integer.MAX_VALUE);
        assertThat(p.get("elasticsearch.slowlog.source"), containsString("{\\\"foo\\\":\\\"bar\\\"}"));
    }

    public void testEmptyRoutingField() throws IOException {
        BytesReference source = BytesReference.bytes(JsonXContent.contentBuilder().startObject().field("foo", "bar").endObject());
        ParsedDocument pd = new ParsedDocument(
            new NumericDocValuesField("version", 1),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            "id",
            null,
            null,
            source,
            XContentType.JSON,
            null
        );
        Index index = new Index("foo", "123");

        ESLogMessage p = IndexingSlowLogMessage.of(index, pd, 10, true, 0);
        assertThat(p.get("routing"), nullValue());
    }

    public void testSlowLogParsedDocumentPrinterSourceToLog() throws IOException {
        BytesReference source = BytesReference.bytes(JsonXContent.contentBuilder().startObject().field("foo", "bar").endObject());
        ParsedDocument pd = new ParsedDocument(
            new NumericDocValuesField("version", 1),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            "id",
            null,
            null,
            source,
            XContentType.JSON,
            null
        );
        Index index = new Index("foo", "123");
        // Turning off document logging doesn't log source[]
        ESLogMessage p = IndexingSlowLogMessage.of(index, pd, 10, true, 0);
        assertThat(p.getFormattedMessage(), not(containsString("source[")));

        // Turning on document logging logs the whole thing
        p = IndexingSlowLogMessage.of(index, pd, 10, true, Integer.MAX_VALUE);
        assertThat(p.get("elasticsearch.slowlog.source"), equalTo("{\\\"foo\\\":\\\"bar\\\"}"));

        // And you can truncate the source
        p = IndexingSlowLogMessage.of(index, pd, 10, true, 3);
        assertThat(p.get("elasticsearch.slowlog.source"), equalTo("{\\\"f"));

        // And you can truncate the source
        p = IndexingSlowLogMessage.of(index, pd, 10, true, 3);
        assertThat(p.get("elasticsearch.slowlog.source"), containsString("{\\\"f"));
        assertThat(p.get("elasticsearch.slowlog.message"), startsWith("[foo/123]"));
        assertThat(p.get("elasticsearch.slowlog.took"), containsString("10nanos"));

        // Throwing a error if source cannot be converted
        source = new BytesArray("invalid");
        ParsedDocument doc = new ParsedDocument(
            new NumericDocValuesField("version", 1),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            "id",
            null,
            null,
            source,
            XContentType.JSON,
            null
        );

        final XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> IndexingSlowLogMessage.of(index, doc, 10, true, 3)
        );
        assertThat(
            e,
            hasToString(
                containsString(
                    "Unrecognized token 'invalid':"
                        + " was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n"
                        + " at [Source: "
                )
            )
        );
    }

    public void testReformatSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING.getKey(), false)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        IndexingSlowLog log = new IndexingSlowLog(settings);
        assertFalse(log.isReformat());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING.getKey(), "true").build())
        );
        assertTrue(log.isReformat());

        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING.getKey(), "false").build())
        );
        assertFalse(log.isReformat());

        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertTrue(log.isReformat());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        log = new IndexingSlowLog(settings);
        assertTrue(log.isReformat());
        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING.getKey(), "NOT A BOOLEAN").build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            final String expected = "illegal value can't update [index.indexing.slowlog.reformat] from [true] to [NOT A BOOLEAN]";
            assertThat(ex, hasToString(containsString(expected)));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            final IllegalArgumentException cause = (IllegalArgumentException) ex.getCause();
            assertThat(cause, hasToString(containsString("Failed to parse value [NOT A BOOLEAN] as only [true] or [false] are allowed.")));
        }
        assertTrue(log.isReformat());
    }

    public void testSetLevels() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING.getKey(), "100ms")
                .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING.getKey(), "200ms")
                .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING.getKey(), "300ms")
                .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING.getKey(), "400ms")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        IndexingSlowLog log = new IndexingSlowLog(settings);
        assertEquals(TimeValue.timeValueMillis(100).nanos(), log.getIndexTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), log.getIndexDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), log.getIndexInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(400).nanos(), log.getIndexWarnThreshold());

        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING.getKey(), "120ms")
                    .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING.getKey(), "220ms")
                    .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING.getKey(), "320ms")
                    .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING.getKey(), "420ms")
                    .build()
            )
        );

        assertEquals(TimeValue.timeValueMillis(120).nanos(), log.getIndexTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(220).nanos(), log.getIndexDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(320).nanos(), log.getIndexInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(420).nanos(), log.getIndexWarnThreshold());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings.updateIndexMetadata(metadata);
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getIndexTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getIndexDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getIndexInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getIndexWarnThreshold());

        settings = new IndexSettings(metadata, Settings.EMPTY);
        log = new IndexingSlowLog(settings);

        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getIndexTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getIndexDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getIndexInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getIndexWarnThreshold());
        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.indexing.slowlog.threshold.index.trace");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.indexing.slowlog.threshold.index.debug");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.indexing.slowlog.threshold.index.info");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.indexing.slowlog.threshold.index.warn");
        }
    }

    private void assertTimeValueException(final IllegalArgumentException e, final String key) {
        final String expected = "illegal value can't update [" + key + "] from [-1] to [NOT A TIME VALUE]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String causeExpected = "failed to parse setting ["
            + key
            + "] with value [NOT A TIME VALUE] as a time value: unit is missing or unrecognized";
        assertThat(cause, hasToString(containsString(causeExpected)));
    }

    private IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(indexSettings)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(name).settings(build).build();
        return metadata;
    }
}
