/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceRowXContentParser;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * How a test indexes its documents. {@link #DOCUMENT} adds each parsed document with {@code IndexWriter#addDocuments}. {@link #BATCH}
 * maps whole columns and adds them with {@code IndexWriter#addBatch}, the way the engine does, and falls back to adding the parsed rows
 * of the batch as documents when the mappers refuse it, as the shard does. The {@link Outcome} says which one executed, so that a
 * fallback is never taken for batch coverage.
 */
public enum IndexingStrategy {
    DOCUMENT {
        @Override
        public Outcome index(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
            throws IOException {
            for (int i = 0; i < sources.size(); i++) {
                final SourceToParse source = new SourceToParse("d" + (firstSeqNo + i), new BytesArray(sources.get(i)), XContentType.JSON);
                addDocument(mapperService, indexWriter, source, firstSeqNo + i);
            }
            return new Outcome(false, null);
        }
    },
    BATCH {
        @Override
        public Outcome index(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
            throws IOException {
            return indexAsBatch(mapperService, indexWriter, sources, firstSeqNo);
        }
    };

    /**
     * @param batched        whether the documents were added with {@code IndexWriter#addBatch}
     * @param fallbackReason why {@link #BATCH} fell back to document indexing, or {@code null} when it did not
     */
    public record Outcome(boolean batched, String fallbackReason) {}

    /**
     * Indexes the sources as documents {@code d<firstSeqNo>} onwards, with sequence numbers starting at {@code firstSeqNo}, primary term 1
     * and version 1. {@code BATCH} lets any exception other than a refusal to batch propagate.
     */
    public abstract Outcome index(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
        throws IOException;

    // NOTE: runs ShardBatchMapper#mapColumnBatch itself, so that the metadata mappers, the required field check and the chunk context
    // are the ones the engine uses. It falls back to document indexing on any Exception and only logs it. A refused resolution and the
    // UnsupportedOperationException that mappers throw to ask for the fallback fall back here too. The logged exception of any other
    // failure, which is a mapper bug, fails the test instead of being hidden by document indexing. A fallback parses the rows of the
    // encoded batch, as the shard does, and not the original JSON, so that it also covers what the row parser does with a null. The logger
    // is raised to WARN while it runs, because the warning carries the exception and a test run can set a higher level.
    private static Outcome indexAsBatch(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
        throws IOException {
        final int count = sources.size();
        final BytesReference[] bytes = new BytesReference[count];
        final BulkItemRequest[] items = new BulkItemRequest[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = new BytesArray(sources.get(i).getBytes(StandardCharsets.UTF_8));
            items[i] = new BulkItemRequest(i, new IndexRequest("index").id("d" + (firstSeqNo + i)).source(bytes[i], XContentType.JSON));
        }
        final MappingLookup lookup = mapperService.mappingLookup();
        final IndexSettings indexSettings = mapperService.getIndexSettings();
        try (EscfBatch escfBatch = EscfEncoder.encode(Arrays.asList(bytes), XContentType.JSON)) {
            final ShardBatchMapper.BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(escfBatch, lookup, indexSettings);
            if (resolution == null) {
                return indexRows(mapperService, indexWriter, escfBatch, firstSeqNo, "resolveMappers refused the batch");
            }
            final FailureRecorder failure = new FailureRecorder();
            final EngineBatch engineBatch;
            final Logger logger = LogManager.getLogger(ShardBatchMapper.class);
            final Level level = logger.getLevel();
            final boolean raiseLevel = logger.isWarnEnabled() == false;
            if (raiseLevel) {
                Loggers.setLevel(logger, Level.WARN);
            }
            try (MockLog mockLog = MockLog.capture(ShardBatchMapper.class)) {
                mockLog.addExpectation(failure);
                engineBatch = ShardBatchMapper.mapColumnBatch(
                    items,
                    escfBatch,
                    new TestShardContext(mapperService),
                    0,
                    count,
                    resolution,
                    Engine.Operation.Origin.PRIMARY,
                    new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY)),
                    DocumentParsingProvider.EMPTY_INSTANCE
                );
                mockLog.assertAllExpectationsMatched();
            } finally {
                if (raiseLevel) {
                    Loggers.setLevel(logger, level);
                }
            }
            if (engineBatch == null) {
                if (failure.thrown instanceof UnsupportedOperationException
                    || isIgnoredMalformedDateFallback(lookup, failure.thrown, sources)
                    || isIgnoredMalformedNumberFallback(lookup, failure.thrown, sources)) {
                    return indexRows(mapperService, indexWriter, escfBatch, firstSeqNo, String.valueOf(failure.thrown.getMessage()));
                }
                throw new AssertionError("the mappers failed to map a batch, which is not a request to fall back", failure.thrown);
            }
            try (engineBatch) {
                final MappedColumns columns = engineBatch.columns();
                columns.fillPrimaryTerm(1L);
                for (int i = 0; i < count; i++) {
                    columns.setSeqNo(i, firstSeqNo + i);
                    columns.setVersion(i, 1L);
                }
                indexWriter.addBatch(columns.toColumnBatch());
            }
            return new Outcome(true, null);
        }
    }

    private static final Pattern DATE_PARSE_FAILURE = Pattern.compile("failed to parse date field \\[(.*?)\\] with format");

    // NOTE: DateFieldMapper#doMapColumnBatch does not apply ignore_malformed. It throws the parse failure of a malformed value and relies
    // on the engine falling back after logging it. That one exception is accepted as a fallback only when the value it names is one of
    // the input values and a date field with ignore_malformed cannot parse it, so a failure on a valid value, such as the timestamp, still
    // fails the test. The exception names the value and not the field, so two date fields that differ in their format and share an
    // ignore_malformed one cannot be told apart.
    private static boolean isIgnoredMalformedDateFallback(MappingLookup lookup, Throwable thrown, List<String> sources) {
        if (thrown instanceof IllegalArgumentException == false) {
            return false;
        }
        final Matcher matcher = DATE_PARSE_FAILURE.matcher(String.valueOf(thrown.getMessage()));
        if (matcher.find() == false) {
            return false;
        }
        final String value = matcher.group(1);
        if (decodedStrings(sources).contains(value) == false) {
            return false;
        }
        for (final Mapper mapper : lookup.fieldMappers()) {
            if (mapper instanceof DateFieldMapper date && date.ignoreMalformed()) {
                try {
                    date.fieldType().parse(value);
                } catch (IllegalArgumentException | DateTimeException | ElasticsearchParseException e) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final Pattern NUMBER_PARSE_FAILURE = Pattern.compile("For input string: \"(.*?)\"");

    // NOTE: NumberFieldMapper#doMapColumnBatch maps a UNION column through NumberColumnTransform, which does not apply ignore_malformed.
    // It throws the parse failure of a malformed string and relies on the engine falling back. The same restrictions as for dates apply.
    private static boolean isIgnoredMalformedNumberFallback(MappingLookup lookup, Throwable thrown, List<String> sources) {
        if (thrown instanceof IllegalArgumentException == false) {
            return false;
        }
        final Matcher matcher = NUMBER_PARSE_FAILURE.matcher(String.valueOf(thrown.getMessage()));
        if (matcher.find() == false) {
            return false;
        }
        final String value = matcher.group(1);
        if (decodedStrings(sources).contains(value) == false) {
            return false;
        }
        for (final Mapper mapper : lookup.fieldMappers()) {
            if (mapper instanceof NumberFieldMapper number && number.ignoreMalformed()) {
                try {
                    number.type().parse(value, true);
                } catch (IllegalArgumentException | ElasticsearchParseException e) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Set<String> decodedStrings(List<String> sources) {
        final Set<String> strings = new HashSet<>();
        for (final String source : sources) {
            collectStrings(XContentHelper.convertToMap(XContentType.JSON.xContent(), source, false), strings);
        }
        return strings;
    }

    private static void collectStrings(Object node, Set<String> strings) {
        if (node instanceof String string) {
            strings.add(string);
        } else if (node instanceof Map<?, ?> map) {
            map.values().forEach(value -> collectStrings(value, strings));
        } else if (node instanceof Collection<?> collection) {
            collection.forEach(value -> collectStrings(value, strings));
        }
    }

    private static Outcome indexRows(
        MapperService mapperService,
        IndexWriter indexWriter,
        EscfBatch escfBatch,
        int firstSeqNo,
        String reason
    ) throws IOException {
        final SourceRowXContentParser.SchemaNode schemaTree = SourceRowXContentParser.buildSchemaTree(escfBatch.schema());
        for (int i = 0; i < escfBatch.docCount(); i++) {
            final SourceToParse source = new SourceToParse(
                "d" + (firstSeqNo + i),
                new RowSource(schemaTree, escfBatch.row(i), XContentType.JSON),
                null,
                Map.of(),
                Map.of(),
                XContentMeteringParserDecorator.NOOP,
                null
            );
            addDocument(mapperService, indexWriter, source, firstSeqNo + i);
        }
        return new Outcome(false, reason);
    }

    private static void addDocument(MapperService mapperService, IndexWriter indexWriter, SourceToParse source, int seqNo)
        throws IOException {
        final ParsedDocument parsed = mapperService.documentMapper().parse(source);
        parsed.updateSeqID(seqNo, 1);
        parsed.version().setLongValue(1);
        indexWriter.addDocuments(parsed.docs());
    }

    private static final class FailureRecorder implements MockLog.LoggingExpectation {
        private volatile Throwable thrown;

        @Override
        public void match(LogEvent event) {
            if (event.getThrown() != null) {
                thrown = event.getThrown();
            }
        }

        @Override
        public void assertMatched() {}
    }

    private record TestShardContext(MapperService mapperService) implements BatchMappingShardContext {
        @Override
        public IndexSettings indexSettings() {
            return mapperService.getIndexSettings();
        }

        @Override
        public long getOperationPrimaryTerm() {
            return 1L;
        }

        @Override
        public long getRelativeTimeInNanos() {
            return 0L;
        }
    }
}
