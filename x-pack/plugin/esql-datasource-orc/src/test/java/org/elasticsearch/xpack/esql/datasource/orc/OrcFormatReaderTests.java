/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.lucene.util.BytesRef;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OrcFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        OrcStorageObjectAdapter.clearCacheForTests();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * A reader that treats the given columns as declared-type — the ones whose target came from an explicit declaration
     * and are therefore licensed to coerce (including narrow) toward it. Mirrors what {@code FileSourceFactory} threads
     * from a dataset mapping in production; without it a coercion that is not a pure widening is treated as an inferred
     * clash and the whole column null-fills.
     */
    private OrcFormatReader declaredReader(String... declaredColumns) {
        return (OrcFormatReader) new OrcFormatReader(blockFactory).withDeclaredTypeColumns(Set.of(declaredColumns));
    }

    public void testFormatName() {
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        assertEquals("orc", reader.formatName());
    }

    public void testFileExtensions() {
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        List<String> extensions = reader.fileExtensions();
        assertEquals(1, extensions.size());
        assertTrue(extensions.contains(".orc"));
    }

    public void testDoesNotSupportWholeFileCompression() {
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        assertFalse("ORC requires random access and cannot be wrapped in a whole-file compressor", reader.supportsWholeFileCompression());
    }

    /**
     * Verifies {@link OrcFormatReader#statusSnapshot()} reports populated counters after a real
     * read drains an ORC file. Sibling-parity with
     * {@code NdJsonFormatReaderStatusSnapshotTests} / {@code CsvFormatReaderStatusSnapshotTests};
     * lives here to reuse the Hadoop FileSystem test infrastructure rather than duplicate it.
     */
    public void testStatusSnapshotPopulatedAfterDrain() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            for (int i = 0; i < 3; i++) {
                idCol.vector[i] = i;
                nameCol.setVal(i, ("row-" + i).getBytes(StandardCharsets.UTF_8));
            }
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        // Snapshot before drain: format identifier present, row count at zero.
        var before = reader.statusSnapshot();
        assertEquals("orc", before.format());
        assertEquals(0L, before.rowsEmitted());
        assertEquals(0L, before.readNanos());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                page.releaseBlocks();
            }
        }

        var after = reader.statusSnapshot();
        assertEquals("orc", after.format());
        assertEquals("3 data rows drained from the file", 3L, after.rowsEmitted());
        assertTrue("read_nanos should be > 0 after at least one batch", after.readNanos() > 0);
    }

    public void testReadSchemaFromSimpleOrc() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString())
            .addField("age", TypeDescription.createInt())
            .addField("active", TypeDescription.createBoolean());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            LongColumnVector ageCol = (LongColumnVector) batch.cols[2];
            LongColumnVector activeCol = (LongColumnVector) batch.cols[3];

            idCol.vector[0] = 1L;
            nameCol.setVal(0, "Alice".getBytes(StandardCharsets.UTF_8));
            ageCol.vector[0] = 30;
            activeCol.vector[0] = 1;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        List<Attribute> attributes = metadata.schema();

        assertEquals(4, attributes.size());

        assertEquals("id", attributes.get(0).name());
        assertEquals(DataType.LONG, attributes.get(0).dataType());

        assertEquals("name", attributes.get(1).name());
        assertEquals(DataType.KEYWORD, attributes.get(1).dataType());

        assertEquals("age", attributes.get(2).name());
        assertEquals(DataType.INTEGER, attributes.get(2).dataType());

        assertEquals("active", attributes.get(3).name());
        assertEquals(DataType.BOOLEAN, attributes.get(3).dataType());
    }

    /**
     * Regression: every attribute produced from an ORC schema must be {@link Nullability#TRUE} regardless of the column shape.
     * ORC's {@code TypeDescription} encodes no non-null guarantee at the schema level — per-file non-null observations live
     * only in footer statistics — so defaulting attributes to non-nullable would cause planner rules (e.g. {@code COALESCE}
     * simplification, {@code IS NULL}/{@code IS NOT NULL} rewriting) to drop legitimate null rows. The schema below covers
     * the representative scalar, list and {@code UNSUPPORTED} (binary) mappings of {@code convertOrcTypeToEsql}.
     */
    public void testSchemaAttributesAreAlwaysNullable() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("age", TypeDescription.createInt())
            .addField("name", TypeDescription.createString())
            .addField("score", TypeDescription.createDouble())
            .addField("ratio", TypeDescription.createFloat())
            .addField("active", TypeDescription.createBoolean())
            .addField("created", TypeDescription.createTimestamp())
            .addField("created_utc", TypeDescription.createTimestampInstant())
            .addField("birth", TypeDescription.createDate())
            .addField("price", TypeDescription.createDecimal().withPrecision(10).withScale(2))
            .addField("payload", TypeDescription.createBinary())
            .addField("tags", TypeDescription.createList(TypeDescription.createString()));

        byte[] orcData = createOrcFile(schema, batch -> { batch.size = 0; });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        List<Attribute> attributes = reader.metadata(storageObject).schema();
        assertEquals(schema.getFieldNames().size(), attributes.size());
        for (Attribute attr : attributes) {
            assertEquals("attribute [" + attr.name() + "] should be nullable", Nullability.TRUE, attr.nullable());
        }
    }

    public void testReadDataFromSimpleOrc() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString())
            .addField("score", TypeDescription.createDouble());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            DoubleColumnVector scoreCol = (DoubleColumnVector) batch.cols[2];

            idCol.vector[0] = 1L;
            nameCol.setVal(0, "Alice".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[0] = 95.5;

            idCol.vector[1] = 2L;
            nameCol.setVal(1, "Bob".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[1] = 87.3;

            idCol.vector[2] = 3L;
            nameCol.setVal(2, "Charlie".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[2] = 92.1;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(3, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);

            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(2));
            assertEquals(new BytesRef("Charlie"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(2, new BytesRef()));
            assertEquals(92.1, ((DoubleBlock) page.getBlock(2)).getDouble(2), 0.001);
        });
    }

    public void testBinaryColumnMapsToUnsupported() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("payload", TypeDescription.createBinary());
        byte[] orcData = createOrcFile(schema, batch -> { batch.size = 0; });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        List<Attribute> attributes = reader.metadata(createStorageObject(orcData)).schema();
        assertEquals(1, attributes.size());
        assertEquals(DataType.UNSUPPORTED, attributes.get(0).dataType());
    }

    public void testStringColumnWithInvalidUtf8IsSanitized() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("name", TypeDescription.createString());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[0];
            // 0xF8..0xFF is never a valid UTF-8 lead byte; it is exactly what crashes the TopN Utf8 encoder.
            nameCol.setVal(0, new byte[] { (byte) 0xFF, (byte) 0xF8 });
            nameCol.setVal(1, "ok".getBytes(StandardCharsets.UTF_8));
        });

        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        StorageObject storageObject = createStorageObject(orcData);
        assertEquals(DataType.KEYWORD, reader.metadata(storageObject).schema().get(0).dataType());

        readFirstPage(reader, storageObject, null, page -> {
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            // Two invalid lead bytes -> two U+FFFD.
            assertEquals(new BytesRef("\uFFFD\uFFFD"), block.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("ok"), block.getBytesRef(1, new BytesRef()));
        });
    }

    public void testReadWithColumnProjection() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString())
            .addField("score", TypeDescription.createDouble());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            DoubleColumnVector scoreCol = (DoubleColumnVector) batch.cols[2];

            idCol.vector[0] = 1L;
            nameCol.setVal(0, "Alice".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[0] = 95.5;

            idCol.vector[1] = 2L;
            nameCol.setVal(1, "Bob".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[1] = 87.3;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, List.of("name", "score"), page -> {
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.001);

            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(1)).getDouble(1), 0.001);
        });
    }

    public void testProjectedColumnMissingFromFileReturnsNullBlock() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString())
            .addField("score", TypeDescription.createDouble());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            DoubleColumnVector scoreCol = (DoubleColumnVector) batch.cols[2];

            idCol.vector[0] = 1L;
            nameCol.setVal(0, "Alice".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[0] = 95.5;

            idCol.vector[1] = 2L;
            nameCol.setVal(1, "Bob".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[1] = 87.3;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, List.of("id", "nonexistent", "score"), page -> {
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertTrue(page.getBlock(1).isNull(0));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertTrue(page.getBlock(1).isNull(1));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);
        });
    }

    public void testReadWithBatching() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("value", TypeDescription.createInt());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 25;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            LongColumnVector valueCol = (LongColumnVector) batch.cols[1];
            for (int i = 0; i < 25; i++) {
                idCol.vector[i] = i + 1;
                valueCol.vector[i] = (i + 1) * 10;
            }
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        assertEquals(25, countRows(reader, storageObject, null, 10));
    }

    public void testReadBooleanColumn() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("active", TypeDescription.createBoolean());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            LongColumnVector activeCol = (LongColumnVector) batch.cols[1];

            idCol.vector[0] = 1L;
            activeCol.vector[0] = 1;

            idCol.vector[1] = 2L;
            activeCol.vector[1] = 0;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            assertTrue(((BooleanBlock) page.getBlock(1)).getBoolean(0));
            assertFalse(((BooleanBlock) page.getBlock(1)).getBoolean(1));
        });
    }

    public void testReadIntegerColumn() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("count", TypeDescription.createInt());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector countCol = (LongColumnVector) batch.cols[0];
            countCol.vector[0] = 100;
            countCol.vector[1] = 200;
            countCol.vector[2] = 300;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(3, page.getPositionCount());
            assertEquals(100, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(200, ((IntBlock) page.getBlock(0)).getInt(1));
            assertEquals(300, ((IntBlock) page.getBlock(0)).getInt(2));
        });
    }

    public void testReadFloatColumn() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("temperature", TypeDescription.createFloat());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            DoubleColumnVector tempCol = (DoubleColumnVector) batch.cols[0];
            tempCol.vector[0] = 98.6;
            tempCol.vector[1] = 37.0;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            assertEquals(98.6, ((DoubleBlock) page.getBlock(0)).getDouble(0), 0.1);
            assertEquals(37.0, ((DoubleBlock) page.getBlock(0)).getDouble(1), 0.1);
        });
    }

    public void testMetadataReturnsCorrectSourceType() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            ((LongColumnVector) batch.cols[0]).vector[0] = 1L;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals("orc", metadata.sourceType());
    }

    public void testReadNullValuesInColumns() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString())
            .addField("score", TypeDescription.createDouble());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            DoubleColumnVector scoreCol = (DoubleColumnVector) batch.cols[2];

            idCol.vector[0] = 1L;
            nameCol.setVal(0, "Alice".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[0] = 95.5;

            idCol.noNulls = false;
            idCol.isNull[1] = true;
            nameCol.noNulls = false;
            nameCol.isNull[1] = true;
            scoreCol.noNulls = false;
            scoreCol.isNull[1] = true;

            idCol.vector[2] = 3L;
            nameCol.setVal(2, "Charlie".getBytes(StandardCharsets.UTF_8));
            scoreCol.vector[2] = 92.1;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(3, page.getPositionCount());

            LongBlock idBlock = (LongBlock) page.getBlock(0);
            assertEquals(1L, idBlock.getLong(0));
            assertTrue(idBlock.isNull(1));
            assertEquals(3L, idBlock.getLong(2));

            BytesRefBlock nameBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
            assertTrue(nameBlock.isNull(1));
            assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(2, new BytesRef()));

            DoubleBlock scoreBlock = (DoubleBlock) page.getBlock(2);
            assertEquals(95.5, scoreBlock.getDouble(0), 0.001);
            assertTrue(scoreBlock.isNull(1));
            assertEquals(92.1, scoreBlock.getDouble(2), 0.001);
        });
    }

    public void testReadRepeatingVectors() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("constant", TypeDescription.createInt());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 5;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            LongColumnVector constantCol = (LongColumnVector) batch.cols[1];

            for (int i = 0; i < 5; i++) {
                idCol.vector[i] = i + 1;
            }
            constantCol.isRepeating = true;
            constantCol.vector[0] = 42;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(5, page.getPositionCount());

            LongBlock idBlock = (LongBlock) page.getBlock(0);
            for (int i = 0; i < 5; i++) {
                assertEquals(i + 1, idBlock.getLong(i));
            }

            IntBlock constantBlock = (IntBlock) page.getBlock(1);
            for (int i = 0; i < 5; i++) {
                assertEquals(42, constantBlock.getInt(i));
            }
        });
    }

    public void testReadTimestampColumn() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("event_time", TypeDescription.createTimestampInstant());

        long epochMillis = Instant.parse("2024-01-15T10:30:00Z").toEpochMilli();

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            TimestampColumnVector tsCol = (TimestampColumnVector) batch.cols[1];

            idCol.vector[0] = 1L;
            tsCol.time[0] = epochMillis;
            tsCol.nanos[0] = 0;

            idCol.vector[1] = 2L;
            tsCol.time[1] = epochMillis + 3600_000;
            tsCol.nanos[1] = 0;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATETIME, metadata.schema().get(1).dataType());

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            LongBlock tsBlock = (LongBlock) page.getBlock(1);
            assertEquals(epochMillis, tsBlock.getLong(0));
            assertEquals(epochMillis + 3600_000, tsBlock.getLong(1));
        });
    }

    public void testReadTimestampColumnWithNulls() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("event_time", TypeDescription.createTimestampInstant());

        long epochMillis = Instant.parse("2024-01-15T10:30:00Z").toEpochMilli();

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            TimestampColumnVector tsCol = (TimestampColumnVector) batch.cols[1];

            idCol.vector[0] = 1L;
            tsCol.time[0] = epochMillis;
            tsCol.nanos[0] = 0;

            idCol.vector[1] = 2L;
            tsCol.noNulls = false;
            tsCol.isNull[1] = true;

            idCol.vector[2] = 3L;
            tsCol.time[2] = epochMillis + 7200_000;
            tsCol.nanos[2] = 0;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(3, page.getPositionCount());
            LongBlock tsBlock = (LongBlock) page.getBlock(1);
            assertEquals(epochMillis, tsBlock.getLong(0));
            assertTrue(tsBlock.isNull(1));
            assertEquals(epochMillis + 7200_000, tsBlock.getLong(2));
        });
    }

    public void testReadDateColumn() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("event_date", TypeDescription.createDate());

        long daysSinceEpoch = 19723L;

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            LongColumnVector dateCol = (LongColumnVector) batch.cols[1];

            idCol.vector[0] = 1L;
            dateCol.vector[0] = daysSinceEpoch;

            idCol.vector[1] = 2L;
            dateCol.vector[1] = daysSinceEpoch + 1;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            LongBlock dateBlock = (LongBlock) page.getBlock(1);
            assertEquals(daysSinceEpoch * 86_400_000L, dateBlock.getLong(0));
            assertEquals((daysSinceEpoch + 1) * 86_400_000L, dateBlock.getLong(1));
        });
    }

    public void testReadListColumnAsMultiValue() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("tags", TypeDescription.createList(TypeDescription.createString()));

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            ListColumnVector tagsCol = (ListColumnVector) batch.cols[1];
            tagsCol.childCount = 3;
            BytesColumnVector tagsChild = (BytesColumnVector) tagsCol.child;

            tagsChild.ensureSize(3, false);
            idCol.vector[0] = 1L;
            tagsCol.offsets[0] = 0;
            tagsCol.lengths[0] = 2;
            tagsChild.setVal(0, "a".getBytes(StandardCharsets.UTF_8));
            tagsChild.setVal(1, "b".getBytes(StandardCharsets.UTF_8));

            idCol.vector[1] = 2L;
            tagsCol.offsets[1] = 2;
            tagsCol.lengths[1] = 1;
            tagsChild.setVal(2, "x".getBytes(StandardCharsets.UTF_8));
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.KEYWORD, metadata.schema().get(1).dataType());

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            LongBlock idBlock = (LongBlock) page.getBlock(0);
            assertEquals(1L, idBlock.getLong(0));
            assertEquals(2L, idBlock.getLong(1));

            BytesRefBlock tagsBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, tagsBlock.getValueCount(0));
            assertEquals(new BytesRef("a"), tagsBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("b"), tagsBlock.getBytesRef(1, new BytesRef()));
            assertEquals(1, tagsBlock.getValueCount(1));
            assertEquals(new BytesRef("x"), tagsBlock.getBytesRef(2, new BytesRef()));
        });
    }

    public void testIteratorCloseReleasesResourcesOnPartialRead() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 5;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            for (int i = 0; i < 5; i++) {
                idCol.vector[i] = i + 1;
            }
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 2)) {
            assertTrue(iterator.hasNext());
            Page page1 = iterator.next();
            assertEquals(2, page1.getPositionCount());
            page1.releaseBlocks();

            assertTrue(iterator.hasNext());
            Page page2 = iterator.next();
            assertEquals(2, page2.getPositionCount());
            page2.releaseBlocks();

            assertTrue(iterator.hasNext());
            Page page3 = iterator.next();
            assertEquals(1, page3.getPositionCount());
            page3.releaseBlocks();

            assertFalse(iterator.hasNext());
        }
    }

    // --- Pushdown tests ---

    /**
     * {@link OrcPushedExpressions#MAX_STRUCT_FLATTENING_DEPTH} re-declares the reader-side constant
     * to keep the pushdown package free of reader-internal coupling. This regression test asserts
     * the two values agree so a path the flattener emits as UNSUPPORTED cannot end up with a
     * pushdown column-type entry that no ESQL attribute can ever bind to.
     */
    public void testStructFlatteningDepthCapAgreesAcrossReaderAndPushdown() {
        assertEquals(OrcFormatReader.MAX_STRUCT_FLATTENING_DEPTH, OrcPushedExpressions.MAX_STRUCT_FLATTENING_DEPTH);
    }

    public void testWithPushedFilterReturnsNewInstance() {
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().equals("id", PredicateLeaf.Type.LONG, 1L).end().build();
        FormatReader withFilter = reader.withPushedFilter(sarg);
        assertNotSame("withPushedFilter must return a new instance", reader, withFilter);
    }

    public void testWithPushedFilterNullReturnsThis() {
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        assertSame("withPushedFilter(null) must return same instance", reader, reader.withPushedFilter(null));
    }

    public void testReadWithPushedFilterMatchingAll() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 5;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            for (int i = 0; i < 5; i++) {
                idCol.vector[i] = i + 1;
            }
        });

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startNot().lessThanEquals("id", PredicateLeaf.Type.LONG, 0L).end().build();

        OrcFormatReader reader = (OrcFormatReader) new OrcFormatReader(blockFactory).withPushedFilter(sarg);
        StorageObject storageObject = createStorageObject(orcData);
        readFirstPage(reader, storageObject, null, page -> assertEquals(5, page.getPositionCount()));
    }

    public void testReadWithPushedFilterMatchingNone() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 5;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            for (int i = 0; i < 5; i++) {
                idCol.vector[i] = i + 1;
            }
        });

        SearchArgument sarg = SearchArgumentFactory.newBuilder()
            .startNot()
            .lessThanEquals("id", PredicateLeaf.Type.LONG, 100L)
            .end()
            .build();

        OrcFormatReader reader = (OrcFormatReader) new OrcFormatReader(blockFactory).withPushedFilter(sarg);
        StorageObject storageObject = createStorageObject(orcData);
        try (CloseableIterator<Page> iter = reader.read(storageObject, null, 1024)) {
            assertFalse("No rows should match the filter", iter.hasNext());
        }
    }

    public void testReadWithPushedFilterAndColumnProjection() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString());

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            idCol.vector[0] = 1L;
            nameCol.setVal(0, "Alice".getBytes(StandardCharsets.UTF_8));
            idCol.vector[1] = 2L;
            nameCol.setVal(1, "Bob".getBytes(StandardCharsets.UTF_8));
            idCol.vector[2] = 3L;
            nameCol.setVal(2, "Charlie".getBytes(StandardCharsets.UTF_8));
        });

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startNot().lessThan("id", PredicateLeaf.Type.LONG, 1L).end().build();

        OrcFormatReader reader = (OrcFormatReader) new OrcFormatReader(blockFactory).withPushedFilter(sarg);
        StorageObject storageObject = createStorageObject(orcData);
        readFirstPage(reader, storageObject, List.of("name"), page -> {
            assertEquals(3, page.getPositionCount());
            assertEquals(1, page.getBlockCount());
            BytesRefBlock nameBlock = (BytesRefBlock) page.getBlock(0);
            assertEquals("Alice", nameBlock.getBytesRef(0, new BytesRef()).utf8ToString());
        });
    }

    public void testTimestampTruncatesToMillisPrecision() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("event_time", TypeDescription.createTimestampInstant());

        long epochMillis = Instant.parse("2024-01-15T10:30:00.123Z").toEpochMilli();

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            TimestampColumnVector tsCol = (TimestampColumnVector) batch.cols[0];
            tsCol.time[0] = epochMillis;
            tsCol.nanos[0] = 123_456_789;
            tsCol.time[1] = epochMillis;
            tsCol.nanos[1] = 123_000_000;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            LongBlock tsBlock = (LongBlock) page.getBlock(0);
            assertEquals(epochMillis, tsBlock.getLong(0));
            assertEquals(epochMillis, tsBlock.getLong(1));
        });
    }

    public void testPreEpochTimestamp() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("birth_date", TypeDescription.createTimestampInstant());

        long preEpochMillis = Instant.parse("1953-09-02T00:00:00Z").toEpochMilli();
        long postEpochMillis = Instant.parse("1986-06-26T00:00:00Z").toEpochMilli();

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            TimestampColumnVector tsCol = (TimestampColumnVector) batch.cols[1];

            idCol.vector[0] = 1L;
            tsCol.time[0] = preEpochMillis;
            tsCol.nanos[0] = 0;

            idCol.vector[1] = 2L;
            tsCol.time[1] = postEpochMillis;
            tsCol.nanos[1] = 0;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATETIME, metadata.schema().get(1).dataType());

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            LongBlock tsBlock = (LongBlock) page.getBlock(1);
            assertTrue("pre-epoch millis should be negative", tsBlock.getLong(0) < 0);
            assertEquals(preEpochMillis, tsBlock.getLong(0));
            assertEquals(postEpochMillis, tsBlock.getLong(1));
        });
    }

    public void testReadListTimestampColumn() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("events", TypeDescription.createList(TypeDescription.createTimestampInstant()));

        long ts1 = Instant.parse("2024-01-15T10:00:00Z").toEpochMilli();
        long ts2 = Instant.parse("2024-01-15T11:00:00Z").toEpochMilli();
        long ts3 = Instant.parse("1965-03-20T08:30:00Z").toEpochMilli();

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            ListColumnVector eventsCol = (ListColumnVector) batch.cols[1];
            TimestampColumnVector eventsChild = (TimestampColumnVector) eventsCol.child;

            eventsChild.ensureSize(3, false);
            idCol.vector[0] = 1L;
            eventsCol.offsets[0] = 0;
            eventsCol.lengths[0] = 2;
            eventsChild.time[0] = ts1;
            eventsChild.nanos[0] = 0;
            eventsChild.time[1] = ts2;
            eventsChild.nanos[1] = 0;

            idCol.vector[1] = 2L;
            eventsCol.offsets[1] = 2;
            eventsCol.lengths[1] = 1;
            eventsChild.time[2] = ts3;
            eventsChild.nanos[2] = 0;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATETIME, metadata.schema().get(1).dataType());

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            LongBlock eventsBlock = (LongBlock) page.getBlock(1);
            assertEquals(2, eventsBlock.getValueCount(0));
            assertEquals(ts1, eventsBlock.getLong(0));
            assertEquals(ts2, eventsBlock.getLong(1));
            assertEquals(1, eventsBlock.getValueCount(1));
            assertTrue("pre-epoch list element should be negative", eventsBlock.getLong(2) < 0);
            assertEquals(ts3, eventsBlock.getLong(2));
        });
    }

    public void testBinaryMapsToUnsupported() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("payload", TypeDescription.createBinary());

        byte[] rawBytes = new byte[] { 0x00, 0x01, (byte) 0xFF, (byte) 0xFE, 0x42 };
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            ((LongColumnVector) batch.cols[0]).vector[0] = 1L;
            ((BytesColumnVector) batch.cols[1]).setVal(0, rawBytes);
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.UNSUPPORTED, metadata.schema().get(1).dataType());

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(1, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertTrue(page.getBlock(1).isNull(0));
        });
    }

    public void testReadDecimalColumn() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("price", TypeDescription.createDecimal().withPrecision(10).withScale(2));

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            DecimalColumnVector priceCol = (DecimalColumnVector) batch.cols[1];

            idCol.vector[0] = 1L;
            priceCol.set(0, new HiveDecimalWritable("123.45"));

            idCol.vector[1] = 2L;
            priceCol.set(1, new HiveDecimalWritable("0.01"));

            idCol.vector[2] = 3L;
            priceCol.set(2, new HiveDecimalWritable("99999.99"));
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DOUBLE, metadata.schema().get(1).dataType());

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(3, page.getPositionCount());
            DoubleBlock priceBlock = (DoubleBlock) page.getBlock(1);
            assertEquals(123.45, priceBlock.getDouble(0), 0.001);
            assertEquals(0.01, priceBlock.getDouble(1), 0.001);
            assertEquals(99999.99, priceBlock.getDouble(2), 0.001);
        });
    }

    public void testReadDecimalColumnWithNulls() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("amount", TypeDescription.createDecimal().withPrecision(10).withScale(2));

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            DecimalColumnVector amountCol = (DecimalColumnVector) batch.cols[0];
            amountCol.set(0, new HiveDecimalWritable("42.50"));
            amountCol.noNulls = false;
            amountCol.isNull[1] = true;
            amountCol.set(2, new HiveDecimalWritable("100.00"));
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(3, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(42.50, block.getDouble(0), 0.001);
            assertTrue(block.isNull(1));
            assertEquals(100.00, block.getDouble(2), 0.001);
        });
    }

    public void testReadDecimalHighPrecision() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("value", TypeDescription.createDecimal().withPrecision(38).withScale(10));

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            DecimalColumnVector valCol = (DecimalColumnVector) batch.cols[0];
            valCol.set(0, new HiveDecimalWritable("1234567890.1234567890"));
            valCol.set(1, new HiveDecimalWritable("-9876543210.0000000001"));
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(1234567890.1234567890, block.getDouble(0), 0.01);
            assertEquals(-9876543210.0000000001, block.getDouble(1), 0.01);
        });
    }

    public void testReadListDecimalColumn() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("prices", TypeDescription.createList(TypeDescription.createDecimal().withPrecision(10).withScale(2)));

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            ListColumnVector pricesCol = (ListColumnVector) batch.cols[1];
            DecimalColumnVector pricesChild = (DecimalColumnVector) pricesCol.child;

            pricesChild.ensureSize(3, false);
            idCol.vector[0] = 1L;
            pricesCol.offsets[0] = 0;
            pricesCol.lengths[0] = 2;
            pricesChild.set(0, new HiveDecimalWritable("10.50"));
            pricesChild.set(1, new HiveDecimalWritable("20.99"));

            idCol.vector[1] = 2L;
            pricesCol.offsets[1] = 2;
            pricesCol.lengths[1] = 1;
            pricesChild.set(2, new HiveDecimalWritable("99.00"));
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DOUBLE, metadata.schema().get(1).dataType());

        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            DoubleBlock pricesBlock = (DoubleBlock) page.getBlock(1);
            assertEquals(2, pricesBlock.getValueCount(0));
            assertEquals(10.50, pricesBlock.getDouble(0), 0.001);
            assertEquals(20.99, pricesBlock.getDouble(1), 0.001);
            assertEquals(1, pricesBlock.getValueCount(1));
            assertEquals(99.00, pricesBlock.getDouble(2), 0.001);
        });
    }

    // --- Range-aware (stripe-level split) tests ---

    public void testDiscoverSplitRanges_singleStripe() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            idCol.vector[0] = 1L;
            idCol.vector[1] = 2L;
            idCol.vector[2] = 3L;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        List<SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertEquals("Single-stripe file should return one range with stats", 1, ranges.size());
        SplitRange range = ranges.getFirst();
        assertTrue("Range offset must be non-negative", range.offset() >= 0);
        assertTrue("Range length must be positive", range.length() > 0);
        assertNotNull("Statistics should be present", range.statistics());
        assertEquals(3L, range.statistics().get("_stats.row_count"));
        assertEquals(0L, range.statistics().get("_stats.columns.id.null_count"));
        assertNotNull("Column min should be present", range.statistics().get("_stats.columns.id.min"));
        assertNotNull("Column max should be present", range.statistics().get("_stats.columns.id.max"));
    }

    public void testDiscoverSplitRanges_emptyFile() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> batch.size = 0);

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        List<SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Empty file (no stripes) should return empty ranges", ranges.isEmpty());
    }

    public void testDiscoverSplitRanges_multiStripeFile() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString());

        byte[] orcData = createMultiStripeOrcFile(schema, 3, batchIndex -> {
            VectorizedRowBatch batch = schema.createRowBatch();
            batch.size = 100;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            for (int i = 0; i < 100; i++) {
                idCol.vector[i] = batchIndex * 100L + i;
                nameCol.setVal(i, ("name_" + (batchIndex * 100 + i)).getBytes(StandardCharsets.UTF_8));
            }
            return batch;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        List<SplitRange> ranges = reader.discoverSplitRanges(storageObject);

        assertTrue("Multi-stripe file should return non-empty ranges", ranges.size() >= 2);
        for (SplitRange range : ranges) {
            assertTrue("Offset should be non-negative", range.offset() >= 0);
            assertTrue("Length should be positive", range.length() > 0);
            assertNotNull("Per-stripe statistics should be present", range.statistics());
            assertTrue("Statistics should contain row count", range.statistics().containsKey("_stats.row_count"));
            assertEquals("Each stripe should have 100 rows", 100L, range.statistics().get("_stats.row_count"));
            assertTrue("Statistics should contain size bytes", range.statistics().containsKey("_stats.size_bytes"));
            assertTrue("Statistics should contain per-column stats", range.statistics().containsKey("_stats.columns.id.null_count"));
        }
        for (int i = 1; i < ranges.size(); i++) {
            assertTrue("Ranges should be in ascending offset order", ranges.get(i).offset() > ranges.get(i - 1).offset());
        }
    }

    public void testReadRange_readsOnlyAssignedStripes() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("value", TypeDescription.createInt());

        int rowsPerStripe = 100;
        int stripeCount = 3;
        byte[] orcData = createMultiStripeOrcFile(schema, stripeCount, batchIndex -> {
            VectorizedRowBatch batch = schema.createRowBatch();
            batch.size = rowsPerStripe;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            LongColumnVector valueCol = (LongColumnVector) batch.cols[1];
            for (int i = 0; i < rowsPerStripe; i++) {
                idCol.vector[i] = batchIndex * rowsPerStripe + i;
                valueCol.vector[i] = batchIndex;
            }
            return batch;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        List<SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Should have multiple stripes", ranges.size() >= 2);

        // First stripe: all values should be 0
        int firstStripeRows = countRangeRows(reader, storageObject, ranges.get(0), page -> {
            IntBlock valueBlock = (IntBlock) page.getBlock(1);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals("All rows in first stripe should have value=0", 0, valueBlock.getInt(i));
            }
        });
        assertEquals("First stripe should contain exactly " + rowsPerStripe + " rows", rowsPerStripe, firstStripeRows);

        // Second stripe: all values should be 1
        if (ranges.size() >= 3) {
            forEachRangePage(reader, storageObject, ranges.get(1), page -> {
                IntBlock valueBlock = (IntBlock) page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertEquals("All rows in second stripe should have value=1", 1, valueBlock.getInt(i));
                }
            });
        }

        // Sum of all range reads should equal total rows
        int fullTotal = 0;
        for (SplitRange range : ranges) {
            fullTotal += countRangeRows(reader, storageObject, range);
        }
        assertEquals("Sum of all range reads should equal total rows", rowsPerStripe * stripeCount, fullTotal);
    }

    public void testReadRange_withProjection() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString())
            .addField("score", TypeDescription.createDouble());

        byte[] orcData = createMultiStripeOrcFile(schema, 2, batchIndex -> {
            VectorizedRowBatch batch = schema.createRowBatch();
            batch.size = 100;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            BytesColumnVector nameCol = (BytesColumnVector) batch.cols[1];
            DoubleColumnVector scoreCol = (DoubleColumnVector) batch.cols[2];
            for (int i = 0; i < 100; i++) {
                idCol.vector[i] = batchIndex * 100L + i;
                nameCol.setVal(i, ("n" + i).getBytes(StandardCharsets.UTF_8));
                scoreCol.vector[i] = batchIndex * 100.0 + i;
            }
            return batch;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        List<SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Should have multiple stripes", ranges.size() >= 2);

        readFirstRangePage(reader, storageObject, ranges.get(0), List.of("name", "score"), page -> {
            assertEquals("Projected to 2 columns", 2, page.getBlockCount());
            assertEquals("First stripe should have 100 rows", 100, page.getPositionCount());
            BytesRefBlock nameBlock = (BytesRefBlock) page.getBlock(0);
            assertEquals(new BytesRef("n0"), nameBlock.getBytesRef(0, new BytesRef()));
            DoubleBlock scoreBlock = (DoubleBlock) page.getBlock(1);
            assertEquals(0.0, scoreBlock.getDouble(0), 0.001);
        });
    }

    public void testReadRange_withPredicate() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());

        byte[] orcData = createMultiStripeOrcFile(schema, 3, batchIndex -> {
            VectorizedRowBatch batch = schema.createRowBatch();
            batch.size = 100;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            for (int i = 0; i < 100; i++) {
                idCol.vector[i] = batchIndex * 1000L + i;
            }
            return batch;
        });

        StorageObject storageObject = createStorageObject(orcData);

        SearchArgument sarg = SearchArgumentFactory.newBuilder()
            .startNot()
            .lessThanEquals("id", PredicateLeaf.Type.LONG, 999999L)
            .end()
            .build();
        OrcFormatReader reader = (OrcFormatReader) new OrcFormatReader(blockFactory).withPushedFilter(sarg);

        List<SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Should have multiple stripes", ranges.size() >= 2);

        assertEquals("Filter should exclude all rows in this stripe", 0, countRangeRows(reader, storageObject, ranges.get(0)));
    }

    /**
     * {@code _rowPosition} must be the absolute, file-global row number — split-invariant — both
     * on a full-file read and when the read is restricted to a later stripe via {@code readRange}.
     * This is the substrate {@code _id} is composed from; a stripe-relative regression here would
     * silently re-key every row whenever the split layout changes. Each row's {@code id} column is
     * written equal to its absolute row number, so the assertion is {@code rowPosition == id}.
     */
    public void testRowPositionIsFileGlobalAcrossStripes() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());
        int rowsPerStripe = 100;
        int stripeCount = 3;
        byte[] orcData = createMultiStripeOrcFile(schema, stripeCount, batchIndex -> {
            VectorizedRowBatch batch = schema.createRowBatch();
            batch.size = rowsPerStripe;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            for (int i = 0; i < rowsPerStripe; i++) {
                idCol.vector[i] = batchIndex * (long) rowsPerStripe + i;
            }
            return batch;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        List<String> projection = List.of("id", ColumnExtractor.ROW_POSITION_COLUMN);

        // Full-file read with a batch size that does not align with stripe boundaries.
        int totalRows = 0;
        try (CloseableIterator<Page> iter = reader.read(storageObject, projection, 64)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                LongBlock idBlock = (LongBlock) page.getBlock(0);
                LongBlock rowPosBlock = (LongBlock) page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertEquals("_rowPosition must equal the absolute row number", idBlock.getLong(i), rowPosBlock.getLong(i));
                }
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertEquals(rowsPerStripe * stripeCount, totalRows);

        // Stripe-ranged read of a non-first stripe: positions stay file-global, not range-relative.
        List<SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Should have multiple stripes", ranges.size() >= 2);
        SplitRange secondStripe = ranges.get(1);
        try (
            CloseableIterator<Page> iter = reader.readRange(
                storageObject,
                new RangeReadContext(projection, 64, secondStripe.offset(), secondStripe.offset() + secondStripe.length(), List.of(), null)
            )
        ) {
            while (iter.hasNext()) {
                Page page = iter.next();
                LongBlock idBlock = (LongBlock) page.getBlock(0);
                LongBlock rowPosBlock = (LongBlock) page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertEquals(
                        "_rowPosition on a ranged read must stay file-global, not range-relative",
                        idBlock.getLong(i),
                        rowPosBlock.getLong(i)
                    );
                    assertTrue("second stripe rows start at " + rowsPerStripe, rowPosBlock.getLong(i) >= rowsPerStripe);
                }
                page.releaseBlocks();
            }
        }
    }

    // --- Read template helpers ---

    private void readFirstPage(
        OrcFormatReader reader,
        StorageObject object,
        List<String> projection,
        CheckedConsumer<Page, Exception> check
    ) throws Exception {
        try (CloseableIterator<Page> iter = reader.read(object, projection, 1024)) {
            assertTrue(iter.hasNext());
            check.accept(iter.next());
        }
    }

    private int countRows(OrcFormatReader reader, StorageObject object, List<String> projection, int batchSize) throws Exception {
        int total = 0;
        try (CloseableIterator<Page> iter = reader.read(object, projection, batchSize)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                total += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        return total;
    }

    private void readFirstRangePage(
        OrcFormatReader reader,
        StorageObject object,
        SplitRange range,
        List<String> projection,
        CheckedConsumer<Page, Exception> check
    ) throws Exception {
        try (
            CloseableIterator<Page> iter = reader.readRange(
                object,
                new RangeReadContext(projection, 1024, range.offset(), range.offset() + range.length(), List.of(), null)
            )
        ) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            check.accept(page);
            page.releaseBlocks();
        }
    }

    private void forEachRangePage(OrcFormatReader reader, StorageObject object, SplitRange range, CheckedConsumer<Page, Exception> check)
        throws Exception {
        try (
            CloseableIterator<Page> iter = reader.readRange(
                object,
                new RangeReadContext(null, 1024, range.offset(), range.offset() + range.length(), List.of(), null)
            )
        ) {
            while (iter.hasNext()) {
                Page page = iter.next();
                check.accept(page);
                page.releaseBlocks();
            }
        }
    }

    private int countRangeRows(OrcFormatReader reader, StorageObject object, SplitRange range) throws Exception {
        return countRangeRows(reader, object, range, page -> {});
    }

    private int countRangeRows(OrcFormatReader reader, StorageObject object, SplitRange range, CheckedConsumer<Page, Exception> check)
        throws Exception {
        int total = 0;
        try (
            CloseableIterator<Page> iter = reader.readRange(
                object,
                new RangeReadContext(null, 1024, range.offset(), range.offset() + range.length(), List.of(), null)
            )
        ) {
            while (iter.hasNext()) {
                Page page = iter.next();
                check.accept(page);
                total += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        return total;
    }

    public void testCorruptOrcFileDoesNotProduceElasticsearchException() throws Exception {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 10;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            for (int i = 0; i < 10; i++) {
                idCol.vector[i] = i;
            }
        });

        int corruptStart = 3;
        int corruptEnd = orcData.length / 2;
        java.util.Arrays.fill(orcData, corruptStart, corruptEnd, (byte) 0xFF);

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        Exception ex = expectThrows(Exception.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 100)) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        assertFalse(
            "Corrupt ORC data should not produce ElasticsearchException (HTTP 500), got: " + ex.getClass().getName(),
            ex instanceof org.elasticsearch.ElasticsearchException
        );
    }

    // --- ORC file creation helpers ---

    @FunctionalInterface
    private interface StripeBatchProducer {
        VectorizedRowBatch produce(int stripeIndex) throws IOException;
    }

    @FunctionalInterface
    private interface BatchPopulator {
        void populate(VectorizedRowBatch batch);
    }

    // ===== Declared-type coercion (Hive/Trino-style read-time coercion, unified across formats) =====

    public void testStringToDatetimeCoercesViaSharedScalar() throws Exception {
        // A declared `date` on a STRING ORC column is coerced string->datetime at read time through the SAME
        // DeclaredTypeCoercions.parseDatetimeMillis the text and parquet readers use — assert the decoded value equals
        // that shared scalar directly, proving the coercion is one registry across all formats (not a parallel impl),
        // and equals the ACCESS_LOG epoch the CSV/NDJSON/Parquet tests pin (cross-format equivalence).
        TypeDescription schema = TypeDescription.createStruct().addField("ts", TypeDescription.createString());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            ((BytesColumnVector) batch.cols[0]).setVal(0, "10/Oct/2000:13:55:36 -0700".getBytes(StandardCharsets.UTF_8));
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = (OrcFormatReader) declaredReader("ts").withDeclaredDateFormats(Map.of("ts", "dd/MMM/yyyy:HH:mm:ss Z"));
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME));
        try (
            CloseableIterator<Page> it = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("ts"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertEquals(1, page.getPositionCount());
            long shared = DeclaredTypeCoercions.parseDatetimeMillis(
                "10/Oct/2000:13:55:36 -0700",
                DateFormatter.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
            );
            assertEquals(shared, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(971211336000L, ((LongBlock) page.getBlock(0)).getLong(0));
            page.releaseBlocks();
        }
    }

    public void testLongToDatetimeReinterpretNotDayScaled() throws Exception {
        // A declared `datetime` on a BIGINT (int64) column reinterprets the raw epoch MILLIS (x1) — it must NOT be
        // day-scaled like a native ORC DATE (days x 86_400_000). Value 1 => epoch milli 1, not 86_400_000.
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            idCol.vector[0] = 1L;
            idCol.vector[1] = 2L;
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = declaredReader("id");
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "id", DataType.DATETIME));
        try (
            CloseableIterator<Page> it = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("id"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            page.releaseBlocks();
        }
    }

    public void testLongDeclaredDatetimeHonorsEpochSecondFormat() throws Exception {
        // An ORC int64 column declared `datetime` defuses off the fused epoch-millis reinterpret when it carries a
        // declared `format`: with epoch_second the value 1704067200 reads as epoch SECONDS (1704067200000 millis, routed
        // through castBlock); with NO format the same value takes the fused reinterpret as epoch MILLIS. Pins the
        // ORC-local routing driven by fusedInDecode(LONG, DATETIME, hasDeclaredFormat) — the ORC twin of the parquet pin.
        long token = 1704067200L; // 2024-01-01T00:00:00Z, in seconds
        TypeDescription schema = TypeDescription.createStruct().addField("ts", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            ((LongColumnVector) batch.cols[0]).vector[0] = token;
        });
        StorageObject storageObject = createStorageObject(orcData);
        List<Attribute> asDatetime = List.of(new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME));

        OrcFormatReader withFormat = (OrcFormatReader) declaredReader("ts").withDeclaredDateFormats(Map.of("ts", "epoch_second"));
        try (
            CloseableIterator<Page> it = withFormat.readRange(
                storageObject,
                new RangeReadContext(List.of("ts"), 10, 0, orcData.length, asDatetime, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertEquals("epoch_second format must parse the int64 as seconds", 1704067200000L, ((LongBlock) page.getBlock(0)).getLong(0));
            page.releaseBlocks();
        }
        try (
            CloseableIterator<Page> it = declaredReader("ts").readRange(
                storageObject,
                new RangeReadContext(List.of("ts"), 10, 0, orcData.length, asDatetime, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertEquals("no format must take the fused epoch-millis reinterpret", token, ((LongBlock) page.getBlock(0)).getLong(0));
            page.releaseBlocks();
        }
    }

    public void testLongDeclaredEpochSecondOverflowHonorsErrorPolicy() throws Exception {
        // The epoch-scaling overflow leg of the unit rule: an int64 declared `date` WITH `format: epoch_second` scales
        // the raw seconds to millis (x1000). A value too large to scale (Long.MAX_VALUE seconds) must FAIL PER CELL and
        // route through the reader's error policy — never a bare ArithmeticException escaping castBlock, never a whole-
        // read abort that also drops the good rows. Columnar readers cannot drop a single row, so skip_row degrades to
        // the same null+warn as null_field here (ErrorPolicy Javadoc); only fail_fast aborts.
        long good = 1704067200L; // 2024-01-01T00:00:00Z in seconds -> 1704067200000 millis
        long overflow = Long.MAX_VALUE; // as epoch-seconds this cannot scale to millis
        TypeDescription schema = TypeDescription.createStruct().addField("ts", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector col = (LongColumnVector) batch.cols[0];
            col.vector[0] = good;
            col.vector[1] = overflow;
            col.vector[2] = good;
        });
        List<Attribute> asDatetime = List.of(new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME));

        // null_field and skip_row: the overflow cell nulls, both good rows survive, one Warning per bad cell.
        for (ErrorPolicy lenient : List.of(ErrorPolicy.PERMISSIVE, ErrorPolicy.LENIENT)) {
            OrcFormatReader reader = (OrcFormatReader) declaredReader("ts").withDeclaredDateFormats(Map.of("ts", "epoch_second"));
            try (
                CloseableIterator<Page> it = reader.readRange(
                    createStorageObject(orcData),
                    new RangeReadContext(List.of("ts"), 10, 0, orcData.length, asDatetime, lenient)
                )
            ) {
                Page page = it.next();
                assertEquals("skip_row cannot drop a row in a columnar batch — every position survives", 3, page.getPositionCount());
                LongBlock longs = (LongBlock) page.getBlock(0);
                assertEquals(1704067200000L, longs.getLong(longs.getFirstValueIndex(0)));
                assertTrue("the overflowing epoch-second cell reads as null under " + lenient.mode(), longs.isNull(1));
                assertEquals(1704067200000L, longs.getLong(longs.getFirstValueIndex(2)));
                page.releaseBlocks();
            }
            List<String> warnings = drainWarnings();
            assertFalse("the nulled overflow cell must emit a Warning under " + lenient.mode(), warnings.isEmpty());
            assertTrue("Warning should name the column, got: " + warnings, warnings.toString().contains("[ts]"));
        }

        // fail_fast: the same overflow aborts the read with a sensible per-cell error — not a bare ArithmeticException.
        OrcFormatReader strict = (OrcFormatReader) declaredReader("ts").withDeclaredDateFormats(Map.of("ts", "epoch_second"));
        Exception failure = expectThrows(Exception.class, () -> {
            try (
                CloseableIterator<Page> it = strict.readRange(
                    createStorageObject(orcData),
                    new RangeReadContext(List.of("ts"), 10, 0, orcData.length, asDatetime, ErrorPolicy.STRICT)
                )
            ) {
                while (it.hasNext()) {
                    it.next().releaseBlocks();
                }
            }
        });
        assertFalse(
            "fail_fast overflow must fail with a sensible message, not a bare ArithmeticException",
            failure instanceof ArithmeticException
        );
        assertTrue("fail_fast must not leak coercion warnings", drainWarnings().isEmpty());
    }

    /**
     * Real-data validation against a genuine ClickBench ORC file, whose {@code EventTime} is a bare int64 of Unix
     * seconds — the exact epoch-scaling shape the unit rule targets. Declared {@code {date, format: epoch_second}}, the
     * column must decode seconds to millis and its MIN/MAX/COUNT must match ground truth computed independently with
     * pyarrow. Skipped unless {@code -Dtests.clickbench.real.orc=<path>} points at a real {@code hits_*.orc}, so CI does
     * not carry the bytes. ORC cannot ride {@code FromDatasetIT}'s flat classpath (a commons-lang3 JarHell collision),
     * so this validates the reader's decode arm directly rather than the full query path. Values pinned from
     * {@code orc/zstd/1-stripe-per-file/hits_140.orc}.
     */
    public void testRealClickBenchOrcEventTimeEpochSecond() throws Exception {
        String realPath = System.getProperty("tests.clickbench.real.orc");
        assumeTrue("set -Dtests.clickbench.real.orc to a real hits_*.orc", realPath != null);
        byte[] orcData = Files.readAllBytes(org.elasticsearch.core.PathUtils.get(realPath));

        long expectedCount = 344064L;
        long expectedMinMillis = 1372968000000L; // 2013-07-04T20:00:00Z
        long expectedMaxMillis = 1374436797000L; // 2013-07-21T19:59:57Z

        OrcFormatReader reader = (OrcFormatReader) declaredReader("EventTime").withDeclaredDateFormats(Map.of("EventTime", "epoch_second"));
        List<Attribute> asDatetime = List.of(new ReferenceAttribute(Source.EMPTY, "EventTime", DataType.DATETIME));
        long count = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        try (
            CloseableIterator<Page> it = reader.readRange(
                createStorageObject(orcData),
                new RangeReadContext(List.of("EventTime"), 8192, 0, orcData.length, asDatetime, ErrorPolicy.STRICT)
            )
        ) {
            while (it.hasNext()) {
                Page page = it.next();
                LongBlock block = (LongBlock) page.getBlock(0);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    assertFalse("real EventTime holds no nulls", block.isNull(p));
                    long v = block.getLong(block.getFirstValueIndex(p));
                    count++;
                    min = Math.min(min, v);
                    max = Math.max(max, v);
                }
                page.releaseBlocks();
            }
        }
        assertEquals("row count", expectedCount, count);
        assertEquals("MIN(EventTime) decoded to epoch millis", expectedMinMillis, min);
        assertEquals("MAX(EventTime) decoded to epoch millis", expectedMaxMillis, max);
    }

    public void testInt32ToLongCoerces() throws Exception {
        // An INT32 ORC column read as `long` widens losslessly — a pure widening the inferred path takes with no declared
        // signal (plain reader), so this pins the widening-compatible branch of the null-fill gate, not the declared escape.
        TypeDescription schema = TypeDescription.createStruct().addField("n", TypeDescription.createInt());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector nCol = (LongColumnVector) batch.cols[0];
            nCol.vector[0] = 7L;
            nCol.vector[1] = 42L;
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "n", DataType.LONG));
        try (
            CloseableIterator<Page> it = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("n"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(7L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(42L, ((LongBlock) page.getBlock(0)).getLong(1));
            page.releaseBlocks();
        }
    }

    public void testBigintInferredIntegerNullFillsWholeColumn() throws Exception {
        // The ORC analog of parquet's parquetFfwAllRows / testInt64InferredIntegerNullFillsWholeColumn: an INFERRED
        // INTEGER target over a BIGINT (int64) column must null-fill the whole column, never narrow. A plain (non-declared)
        // reader here pins the inferred branch of the null-fill gate — DeclaredTypeCoercions.supports(LONG, INTEGER) is
        // true, so dropping the declaredTypeColumns guard in validatePlannerTypesAgainstFile would downcast and this fails.
        TypeDescription schema = TypeDescription.createStruct().addField("n", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector nCol = (LongColumnVector) batch.cols[0];
            nCol.vector[0] = 7L;
            nCol.vector[1] = 42L;
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory); // PLAIN reader: no declaredTypeColumns => inferred
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "n", DataType.INTEGER));
        try (
            CloseableIterator<Page> it = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("n"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertEquals(2, page.getPositionCount());
            assertTrue("inferred int64->integer must null-fill the whole column, never downcast", page.getBlock(0).isNull(0));
            assertTrue(page.getBlock(0).isNull(1));
            page.releaseBlocks();
        }
        List<String> warnings = drainWarnings();
        assertFalse("inferred incompatibility must emit a response Warning", warnings.isEmpty());
        assertTrue(
            "warning must name the incompatibility, got: " + warnings,
            warnings.toString().contains("incompatible with planner type")
        );
    }

    public void testLongToDoubleCoerces() throws Exception {
        // A declared `double` on a BIGINT column coerces like bulk ingest of a long into a double field: the user
        // declared it double, so the reader converts at decode time (values beyond 2^53 round exactly like ingest).
        TypeDescription schema = TypeDescription.createStruct().addField("v", TypeDescription.createLong());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            LongColumnVector vCol = (LongColumnVector) batch.cols[0];
            vCol.vector[0] = 1L;
            vCol.vector[1] = -42L;
            vCol.vector[2] = 9007199254740993L;
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "v", DataType.DOUBLE));
        try (
            CloseableIterator<Page> it = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("v"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertEquals(3, page.getPositionCount());
            DoubleBlock doubles = (DoubleBlock) page.getBlock(0);
            assertEquals(1.0, doubles.getDouble(0), 0.0);
            assertEquals(-42.0, doubles.getDouble(1), 0.0);
            assertEquals(9007199254740992.0, doubles.getDouble(2), 0.0);
            page.releaseBlocks();
        }
    }

    public void testStringToLongDoubleBooleanIpCoerces() throws Exception {
        // Columnar string columns coerce into any declared type exactly like the text readers parse them — the
        // mapper-ingest coercion set (string->long/double/boolean/ip), applied at decode via the shared castBlock.
        TypeDescription schema = TypeDescription.createStruct()
            .addField("s_long", TypeDescription.createString())
            .addField("s_double", TypeDescription.createString())
            .addField("s_bool", TypeDescription.createString())
            .addField("s_ip", TypeDescription.createString());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            ((BytesColumnVector) batch.cols[0]).setVal(0, "42".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) batch.cols[1]).setVal(0, "2.5".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) batch.cols[2]).setVal(0, "true".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) batch.cols[3]).setVal(0, "10.20.30.40".getBytes(StandardCharsets.UTF_8));
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = declaredReader("s_long", "s_double", "s_bool", "s_ip");
        List<Attribute> plannerSchema = List.of(
            new ReferenceAttribute(Source.EMPTY, "s_long", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, "s_double", DataType.DOUBLE),
            new ReferenceAttribute(Source.EMPTY, "s_bool", DataType.BOOLEAN),
            new ReferenceAttribute(Source.EMPTY, "s_ip", DataType.IP)
        );
        try (
            CloseableIterator<Page> it = reader.readRange(
                storageObject,
                new RangeReadContext(
                    List.of("s_long", "s_double", "s_bool", "s_ip"),
                    10,
                    0,
                    orcData.length,
                    plannerSchema,
                    ErrorPolicy.STRICT
                )
            )
        ) {
            Page page = it.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(42L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(2.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.0);
            assertTrue(((BooleanBlock) page.getBlock(2)).getBoolean(0));
            assertEquals(StringUtils.parseIP("10.20.30.40"), ((BytesRefBlock) page.getBlock(3)).getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testDefaultErrorPolicyIsStrict() {
        // Guard the columnar default: it must be STRICT (fail_fast) — the base FormatReader default, identical to the
        // text readers (CSV/NDJSON) and Parquet. A per-value coercion failure fails the read unless a query opts into
        // error_mode: null_field. This pins the cross-format consistency and fails if a permissive default is ever
        // re-introduced for this reader.
        assertEquals(ErrorPolicy.STRICT, new OrcFormatReader(blockFactory).defaultErrorPolicy());
    }

    public void testCoercionUnparseableValueEmitsWarningAndNull() throws Exception {
        // Per-cell leniency under an explicit null_field (PERMISSIVE) error policy: a token the declared type
        // cannot coerce nulls THAT cell and records a response Warning header; the surrounding cells still decode.
        // (The default policy is STRICT — see testDefaultErrorPolicyIsStrict — so leniency is opt-in.)
        TypeDescription schema = TypeDescription.createStruct().addField("n", TypeDescription.createString());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            ((BytesColumnVector) batch.cols[0]).setVal(0, "41".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) batch.cols[0]).setVal(1, "oops".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) batch.cols[0]).setVal(2, "43".getBytes(StandardCharsets.UTF_8));
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = declaredReader("n");
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "n", DataType.LONG));
        try (
            CloseableIterator<Page> it = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("n"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.PERMISSIVE)
            )
        ) {
            Page page = it.next();
            assertEquals(3, page.getPositionCount());
            LongBlock longs = (LongBlock) page.getBlock(0);
            assertEquals(41L, longs.getLong(longs.getFirstValueIndex(0)));
            assertTrue("the unparseable cell reads as null", longs.isNull(1));
            assertEquals(43L, longs.getLong(longs.getFirstValueIndex(2)));
            page.releaseBlocks();
        }
        List<String> warnings = drainWarnings();
        // 1 summary + 1 detail
        assertEquals("Expected summary + 1 detail, got: " + warnings, 2, warnings.size());
        assertTrue("Summary should mention coercion, got: " + warnings.get(0), warnings.get(0).contains("coerced"));
        assertTrue("Detail should name the column, got: " + warnings.get(1), warnings.get(1).contains("[n]"));
        assertTrue("Detail should name the declared type, got: " + warnings.get(1), warnings.get(1).contains("[long]"));
    }

    public void testCoercionUnparseableValueFailFastFailsRead() throws Exception {
        // error_mode: fail_fast makes a coercion failure abort the read — the same outcome the
        // text readers produce for the same declared coercion on the same bad token.
        TypeDescription schema = TypeDescription.createStruct().addField("n", TypeDescription.createString());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            ((BytesColumnVector) batch.cols[0]).setVal(0, "41".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) batch.cols[0]).setVal(1, "oops".getBytes(StandardCharsets.UTF_8));
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = declaredReader("n");
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "n", DataType.LONG));
        try (
            CloseableIterator<Page> it = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("n"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
            )
        ) {
            // Reusing the :: cast engine, an unparseable numeric token throws InvalidArgumentException
            // (a QlClientException -> HTTP 400) under fail_fast, not a raw IllegalArgumentException.
            expectThrows(InvalidArgumentException.class, () -> {
                while (it.hasNext()) {
                    it.next().releaseBlocks();
                }
            });
        }
        assertTrue("fail_fast must not emit coercion warnings", drainWarnings().isEmpty());
    }

    public void testStringToDatetimeBadTokenNullFieldWarnsFailFastFails() throws Exception {
        // The FUSED string->datetime arm follows the same per-cell leniency as castBlock: under an explicit
        // null_field policy a bad token -> null cell + Warning and the read succeeds; under fail_fast (also the
        // default) it aborts — it previously hard-failed the read on any policy.
        TypeDescription schema = TypeDescription.createStruct().addField("ts", TypeDescription.createString());
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            ((BytesColumnVector) batch.cols[0]).setVal(0, "2000-10-10T20:55:36Z".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) batch.cols[0]).setVal(1, "not-a-date".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) batch.cols[0]).setVal(2, "2000-10-10T20:55:38Z".getBytes(StandardCharsets.UTF_8));
        });
        OrcFormatReader reader = declaredReader("ts");
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME));
        try (
            CloseableIterator<Page> it = reader.readRange(
                createStorageObject(orcData),
                new RangeReadContext(List.of("ts"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.PERMISSIVE)
            )
        ) {
            Page page = it.next();
            assertEquals(3, page.getPositionCount());
            LongBlock longs = (LongBlock) page.getBlock(0);
            assertEquals(971211336000L, longs.getLong(longs.getFirstValueIndex(0)));
            assertTrue("the bad date token reads as null", longs.isNull(1));
            assertEquals(971211338000L, longs.getLong(longs.getFirstValueIndex(2)));
            page.releaseBlocks();
        }
        List<String> warnings = drainWarnings();
        assertEquals("Expected summary + 1 detail, got: " + warnings, 2, warnings.size());
        assertTrue("Detail should name the column, got: " + warnings.get(1), warnings.get(1).contains("[ts]"));
        assertTrue("Detail should name the declared type, got: " + warnings.get(1), warnings.get(1).contains("[datetime]"));

        try (
            CloseableIterator<Page> it = reader.readRange(
                createStorageObject(orcData),
                new RangeReadContext(List.of("ts"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
            )
        ) {
            expectThrows(InvalidArgumentException.class, () -> {
                while (it.hasNext()) {
                    it.next().releaseBlocks();
                }
            });
        }
        assertTrue("fail_fast must not emit coercion warnings", drainWarnings().isEmpty());
    }

    public void testListStringToDatetimeBadTokenNullsWholePosition() throws Exception {
        // LIST<string> declared datetime: castBlock's bulk semantics on the fused list arm — a bad
        // element nulls the WHOLE position + warns under the default policy, the clean row still
        // decodes; under fail_fast the read fails.
        TypeDescription schema = TypeDescription.createStruct()
            .addField("vals", TypeDescription.createList(TypeDescription.createString()));
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            ListColumnVector valsCol = (ListColumnVector) batch.cols[0];
            valsCol.childCount = 3;
            BytesColumnVector child = (BytesColumnVector) valsCol.child;
            child.ensureSize(3, false);
            valsCol.offsets[0] = 0;
            valsCol.lengths[0] = 1;
            child.setVal(0, "2000-10-10T20:55:36Z".getBytes(StandardCharsets.UTF_8));
            valsCol.offsets[1] = 1;
            valsCol.lengths[1] = 2;
            child.setVal(1, "not-a-date".getBytes(StandardCharsets.UTF_8));
            child.setVal(2, "2000-10-10T20:55:38Z".getBytes(StandardCharsets.UTF_8));
        });
        OrcFormatReader reader = declaredReader("vals");
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "vals", DataType.DATETIME));
        try (
            CloseableIterator<Page> it = reader.readRange(
                createStorageObject(orcData),
                new RangeReadContext(List.of("vals"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.PERMISSIVE)
            )
        ) {
            Page page = it.next();
            assertEquals(2, page.getPositionCount());
            LongBlock longs = (LongBlock) page.getBlock(0);
            assertEquals(971211336000L, longs.getLong(longs.getFirstValueIndex(0)));
            assertTrue("bulk semantics null the whole position, not one element", longs.isNull(1));
            page.releaseBlocks();
        }
        List<String> warnings = drainWarnings();
        assertEquals("Expected summary + 1 detail, got: " + warnings, 2, warnings.size());

        try (
            CloseableIterator<Page> it = reader.readRange(
                createStorageObject(orcData),
                new RangeReadContext(List.of("vals"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
            )
        ) {
            expectThrows(InvalidArgumentException.class, () -> {
                while (it.hasNext()) {
                    it.next().releaseBlocks();
                }
            });
        }
        assertTrue("fail_fast must not emit coercion warnings", drainWarnings().isEmpty());
    }

    public void testListDatetimeNullElementSkippedNotEpochZero() throws Exception {
        // A [value, null] timestamp list reads as the single-element [value] — null elements are
        // skipped, never decoded as epoch 0 — and an all-null list is a null position. The
        // Parquet suite pins the identical expectation
        // (ParquetFormatReaderTests#testListDatetimeNullElementSkippedNotEpochZero) so the two
        // columnar readers agree on the shape. Covers the timestamp child arm; the long and
        // string child arms share the same null-skip in createListDatetimeBlock.
        TypeDescription schema = TypeDescription.createStruct()
            .addField("events", TypeDescription.createList(TypeDescription.createTimestampInstant()));
        long ts = 971211336000L;
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            ListColumnVector eventsCol = (ListColumnVector) batch.cols[0];
            eventsCol.childCount = 3;
            TimestampColumnVector child = (TimestampColumnVector) eventsCol.child;
            child.ensureSize(3, false);
            child.noNulls = false;
            // Row 0: [ts, null]
            eventsCol.offsets[0] = 0;
            eventsCol.lengths[0] = 2;
            child.time[0] = ts;
            child.nanos[0] = 0;
            child.isNull[1] = true;
            // Row 1: [null]
            eventsCol.offsets[1] = 2;
            eventsCol.lengths[1] = 1;
            child.isNull[2] = true;
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());
            LongBlock longs = (LongBlock) page.getBlock(0);
            assertEquals("null element is dropped, not decoded as epoch 0", 1, longs.getValueCount(0));
            assertEquals(ts, longs.getLong(longs.getFirstValueIndex(0)));
            assertTrue("an all-null list is a null position", longs.isNull(1));
        });
    }

    public void testListPrimitiveNullElementSkippedNotZeroFilled() throws Exception {
        // Twin of testListDatetimeNullElementSkippedNotEpochZero for the five primitive list arms
        // (int/long/double/boolean/string). A [value, null] list reads as the single-element
        // [value] — a null child element is skipped, never emitted as a phantom 0 / 0.0 / false /
        // "" — and an all-null list is a null position, matching the Parquet list decode.
        TypeDescription schema = TypeDescription.createStruct()
            .addField("ints", TypeDescription.createList(TypeDescription.createInt()))
            .addField("longs", TypeDescription.createList(TypeDescription.createLong()))
            .addField("dbls", TypeDescription.createList(TypeDescription.createDouble()))
            .addField("flags", TypeDescription.createList(TypeDescription.createBoolean()))
            .addField("tags", TypeDescription.createList(TypeDescription.createString()));
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            for (int c = 0; c < 5; c++) {
                ListColumnVector listCol = (ListColumnVector) batch.cols[c];
                listCol.childCount = 3;
                listCol.child.ensureSize(3, false);
                listCol.child.noNulls = false;
                // Row 0: [value, null]; Row 1: [null]
                listCol.offsets[0] = 0;
                listCol.lengths[0] = 2;
                listCol.child.isNull[1] = true;
                listCol.offsets[1] = 2;
                listCol.lengths[1] = 1;
                listCol.child.isNull[2] = true;
            }
            ((LongColumnVector) ((ListColumnVector) batch.cols[0]).child).vector[0] = 42L;
            ((LongColumnVector) ((ListColumnVector) batch.cols[1]).child).vector[0] = 42L;
            ((DoubleColumnVector) ((ListColumnVector) batch.cols[2]).child).vector[0] = 4.5;
            ((LongColumnVector) ((ListColumnVector) batch.cols[3]).child).vector[0] = 1L; // true
            ((BytesColumnVector) ((ListColumnVector) batch.cols[4]).child).setVal(0, "a".getBytes(StandardCharsets.UTF_8));
        });
        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        readFirstPage(reader, storageObject, null, page -> {
            assertEquals(2, page.getPositionCount());

            IntBlock ints = (IntBlock) page.getBlock(0);
            assertEquals("null element dropped, not 0", 1, ints.getValueCount(0));
            assertEquals(42, ints.getInt(ints.getFirstValueIndex(0)));
            assertTrue("all-null list is a null position", ints.isNull(1));

            LongBlock longs = (LongBlock) page.getBlock(1);
            assertEquals(1, longs.getValueCount(0));
            assertEquals(42L, longs.getLong(longs.getFirstValueIndex(0)));
            assertTrue(longs.isNull(1));

            DoubleBlock dbls = (DoubleBlock) page.getBlock(2);
            assertEquals(1, dbls.getValueCount(0));
            assertEquals(4.5, dbls.getDouble(dbls.getFirstValueIndex(0)), 0.0);
            assertTrue(dbls.isNull(1));

            BooleanBlock flags = (BooleanBlock) page.getBlock(3);
            assertEquals("null element dropped, not false", 1, flags.getValueCount(0));
            assertTrue(flags.getBoolean(flags.getFirstValueIndex(0)));
            assertTrue(flags.isNull(1));

            BytesRefBlock tags = (BytesRefBlock) page.getBlock(4);
            assertEquals("null element dropped, not empty string", 1, tags.getValueCount(0));
            assertEquals(new BytesRef("a"), tags.getBytesRef(tags.getFirstValueIndex(0), new BytesRef()));
            assertTrue(tags.isNull(1));
        });
    }

    /**
     * Guard: every {@code DeclaredTypeCoercions.fusedInDecode} pair must decode NATIVELY in this
     * reader — a fused pair is never routed through {@code castBlock}, so a pair wired into only
     * the Parquet decode loops would fall through {@code createDatetimeBlock}'s
     * {@code newConstantNullBlock} tail (or a sibling default arm) and silent-null here.
     * Enumerates the fused matrix mechanically; a NEW fused pair without a mapping fails loudly
     * so its author must wire the native arm (in BOTH readers — the Parquet suite carries the
     * twin of this guard) and extend the mapping.
     */
    public void testEveryFusedInDecodePairDecodesNatively() throws Exception {
        for (DataType from : DataType.values()) {
            for (DataType to : DataType.values()) {
                if (from == to || DeclaredTypeCoercions.fusedInDecode(from, to) == false) {
                    continue;
                }
                if (from == DataType.TEXT) {
                    continue; // no ORC leaf type maps to TEXT; the pair is unreachable from a file
                }
                TypeDescription schema = TypeDescription.createStruct();
                BatchPopulator populator;
                switch (from) {
                    case INTEGER -> {
                        schema.addField("x", TypeDescription.createInt());
                        populator = batch -> {
                            batch.size = 1;
                            ((LongColumnVector) batch.cols[0]).vector[0] = 41L;
                        };
                    }
                    case LONG -> {
                        schema.addField("x", TypeDescription.createLong());
                        populator = batch -> {
                            batch.size = 1;
                            ((LongColumnVector) batch.cols[0]).vector[0] = 971211336000L;
                        };
                    }
                    case KEYWORD -> {
                        schema.addField("x", TypeDescription.createString());
                        String token = to == DataType.DATETIME ? "2000-10-10T20:55:36Z" : "hello";
                        populator = batch -> {
                            batch.size = 1;
                            ((BytesColumnVector) batch.cols[0]).setVal(0, token.getBytes(StandardCharsets.UTF_8));
                        };
                    }
                    default -> throw new AssertionError(
                        "fused pair "
                            + from.typeName()
                            + "->"
                            + to.typeName()
                            + " has no native-coverage mapping; wire it into the ORC decode arms and this guard"
                    );
                }
                byte[] orcData = createOrcFile(schema, populator);
                List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "x", to));
                OrcFormatReader reader = declaredReader("x");
                try (
                    CloseableIterator<Page> it = reader.readRange(
                        createStorageObject(orcData),
                        new RangeReadContext(List.of("x"), 10, 0, orcData.length, plannerSchema, ErrorPolicy.STRICT)
                    )
                ) {
                    Page page = it.next();
                    String pair = from.typeName() + "->" + to.typeName();
                    assertEquals(1, page.getPositionCount());
                    assertFalse("fused pair " + pair + " must decode natively, not silent-null", page.getBlock(0).isNull(0));
                    switch (to) {
                        case LONG -> assertEquals(pair, 41L, ((LongBlock) page.getBlock(0)).getLong(0));
                        // bigint source reinterprets the raw epoch millis; string source ISO-parses to the same instant
                        case DATETIME -> assertEquals(pair, 971211336000L, ((LongBlock) page.getBlock(0)).getLong(0));
                        case KEYWORD, TEXT -> assertEquals(
                            pair,
                            new BytesRef("hello"),
                            ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef())
                        );
                        default -> throw new AssertionError("fused pair " + pair + " has no expected-value arm in this guard");
                    }
                    page.releaseBlocks();
                }
            }
        }
        assertTrue("native fused decodes must not warn", drainWarnings().isEmpty());
    }

    private List<String> drainWarnings() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        threadContext.stashContext();
        return messages;
    }

    /**
     * Coercion tests emit response Warning headers; drop any accumulated ones so the parent
     * {@code ensureNoWarnings} post-check passes. Tests that assert on them call
     * {@link #drainWarnings()} from inside the test method.
     */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    private byte[] createOrcFile(TypeDescription schema, BatchPopulator populator) throws IOException {
        var tempFile = createTempFile();
        Files.delete(tempFile);
        Path orcPath = new Path(tempFile.toUri());

        Configuration conf = new Configuration(false);
        conf.set("orc.key.provider", "memory");
        NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
        localFs.setConf(conf);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(localFs)
            .compress(CompressionKind.NONE);

        try (Writer writer = OrcFile.createWriter(orcPath, writerOptions)) {
            VectorizedRowBatch batch = schema.createRowBatch();
            populator.populate(batch);
            writer.addRowBatch(batch);
        }

        return Files.readAllBytes(tempFile);
    }

    private byte[] createMultiStripeOrcFile(TypeDescription schema, int stripeCount, StripeBatchProducer producer) throws IOException {
        var tempFile = createTempFile();
        Files.delete(tempFile);
        Path orcPath = new Path(tempFile.toUri());

        Configuration conf = new Configuration(false);
        conf.set("orc.key.provider", "memory");
        NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
        localFs.setConf(conf);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(localFs)
            .compress(CompressionKind.NONE)
            .stripeSize(512);

        try (Writer writer = OrcFile.createWriter(orcPath, writerOptions)) {
            for (int s = 0; s < stripeCount; s++) {
                VectorizedRowBatch batch = producer.produce(s);
                writer.addRowBatch(batch);
                writer.writeIntermediateFooter();
            }
        }

        return Files.readAllBytes(tempFile);
    }

    /**
     * A minimal local FileSystem that avoids Hadoop's {@code Shell} class entirely.
     * {@code Shell.<clinit>} tries to start a process ({@code ProcessBuilder.start}) to check
     * for {@code setsid} support, which is blocked by the entitlement system. We override:
     * <ul>
     *   <li>{@code createOutputStreamWithMode} — the default creates a {@code LocalFSFileOutputStream}
     *       whose constructor triggers {@code IOStatisticsContext → StringUtils → Shell.<clinit>};
     *       we return a plain {@link FileOutputStream} instead.</li>
     *   <li>{@code mkOneDirWithMode} — references {@code Shell.WINDOWS}.</li>
     *   <li>{@code setPermission} — shells out for {@code chmod}.</li>
     * </ul>
     */
    private static class NoPermissionLocalFileSystem extends RawLocalFileSystem {
        @Override
        @SuppressForbidden(reason = "Bypass Hadoop's LocalFSFileOutputStream to avoid Shell.<clinit>")
        protected OutputStream createOutputStreamWithMode(Path p, boolean append, FsPermission permission) throws IOException {
            return new FileOutputStream(pathToFile(p), append);
        }

        @Override
        public void setPermission(Path p, FsPermission permission) {
            // no-op: skip chmod calls that would trigger Shell
        }

        @Override
        @SuppressForbidden(reason = "Hadoop API requires java.io.File in method signature")
        protected boolean mkOneDirWithMode(Path p, File p2f, FsPermission permission) throws IOException {
            return p2f.mkdir() || p2f.isDirectory();
        }
    }

    // --- Nested STRUCT subfield projection tests ---

    public void testNestedStructFlatteningOrc() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField(
                "event",
                TypeDescription.createStruct()
                    .addField("action", TypeDescription.createString())
                    .addField("outcome", TypeDescription.createString())
                    .addField("tags", TypeDescription.createList(TypeDescription.createString()))
            );
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            ((LongColumnVector) batch.cols[0]).vector[0] = 1L;
            var ev = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[1];
            ((BytesColumnVector) ev.fields[0]).setVal(0, "login".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) ev.fields[1]).setVal(0, "success".getBytes(StandardCharsets.UTF_8));
            ListColumnVector tags = (ListColumnVector) ev.fields[2];
            tags.offsets[0] = 0;
            tags.lengths[0] = 2;
            tags.child.ensureSize(2, false);
            ((BytesColumnVector) tags.child).setVal(0, "x".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) tags.child).setVal(1, "y".getBytes(StandardCharsets.UTF_8));
        });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(createStorageObject(orcData));
        List<Attribute> attrs = metadata.schema();
        assertEquals(4, attrs.size());
        assertEquals("id", attrs.get(0).name());
        assertEquals(DataType.LONG, attrs.get(0).dataType());
        assertEquals("event.action", attrs.get(1).name());
        assertEquals(DataType.KEYWORD, attrs.get(1).dataType());
        assertEquals("event.outcome", attrs.get(2).name());
        assertEquals(DataType.KEYWORD, attrs.get(2).dataType());
        assertEquals("event.tags", attrs.get(3).name());
        assertEquals(DataType.KEYWORD, attrs.get(3).dataType());
    }

    public void testReadNestedStructSingleSubfield() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("action", TypeDescription.createString()));
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            var ev = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[0];
            ((BytesColumnVector) ev.fields[0]).setVal(0, "login".getBytes(StandardCharsets.UTF_8));
        });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        try (CloseableIterator<Page> iter = reader.read(createStorageObject(orcData), List.of("event.action"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertEquals(1, page.getBlockCount());
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertEquals(new BytesRef("login"), block.getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testReadNestedStructTwoSubfieldsSameParent() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField(
                "event",
                TypeDescription.createStruct()
                    .addField("action", TypeDescription.createString())
                    .addField("outcome", TypeDescription.createString())
            );
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            var ev = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[0];
            ((BytesColumnVector) ev.fields[0]).setVal(0, "login".getBytes(StandardCharsets.UTF_8));
            ((BytesColumnVector) ev.fields[1]).setVal(0, "success".getBytes(StandardCharsets.UTF_8));
        });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        try (CloseableIterator<Page> iter = reader.read(createStorageObject(orcData), List.of("event.action", "event.outcome"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertEquals(2, page.getBlockCount());
            assertEquals(new BytesRef("login"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("success"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testReadNestedStructMixedTopLevelAndNested() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("event", TypeDescription.createStruct().addField("action", TypeDescription.createString()));
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            ((LongColumnVector) batch.cols[0]).vector[0] = 7L;
            var ev = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[1];
            ((BytesColumnVector) ev.fields[0]).setVal(0, "logout".getBytes(StandardCharsets.UTF_8));
        });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        try (CloseableIterator<Page> iter = reader.read(createStorageObject(orcData), List.of("id", "event.action"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertEquals(2, page.getBlockCount());
            assertEquals(7L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("logout"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testReadNestedStructDeepNesting() throws Exception {
        // a.b.c.d
        TypeDescription schema = TypeDescription.createStruct()
            .addField(
                "a",
                TypeDescription.createStruct()
                    .addField(
                        "b",
                        TypeDescription.createStruct()
                            .addField("c", TypeDescription.createStruct().addField("d", TypeDescription.createLong()))
                    )
            );
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 1;
            var a = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[0];
            var b = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) a.fields[0];
            var c = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) b.fields[0];
            ((LongColumnVector) c.fields[0]).vector[0] = 999L;
        });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        try (CloseableIterator<Page> iter = reader.read(createStorageObject(orcData), List.of("a.b.c.d"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertEquals(999L, ((LongBlock) page.getBlock(0)).getLong(0));
            page.releaseBlocks();
        }
    }

    /**
     * Repeating string leaf below per-row ancestor nulls. ORC's reader sets
     * {@code child.isRepeating = true} when every value in the batch is equal; combined with
     * a parent struct that is null on some rows this exercises the per-row expansion path —
     * without it the conversion would either produce a constant non-null block (losing parent
     * nulls) or a constant-null block (losing all real values).
     */
    public void testReadNestedStructAncestorNullPropagationRepeatingStringChild() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("action", TypeDescription.createString()));
        int rows = 200;
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = rows;
            var ev = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[0];
            BytesColumnVector action = (BytesColumnVector) ev.fields[0];
            ev.noNulls = false;
            byte[] sameValue = "same".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < rows; i++) {
                ev.isNull[i] = (i % 3 == 0); // every 3rd parent is null
                action.setVal(i, sameValue);
            }
        });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        try (CloseableIterator<Page> iter = reader.read(createStorageObject(orcData), List.of("event.action"), 1024)) {
            int seen = 0;
            BytesRef scratch = new BytesRef();
            while (iter.hasNext()) {
                Page page = iter.next();
                BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++, seen++) {
                    if (seen % 3 == 0) {
                        assertTrue("row " + seen + " parent null -> leaf null", block.isNull(i));
                    } else {
                        assertFalse("row " + seen + " parent non-null -> leaf non-null", block.isNull(i));
                        assertEquals(new BytesRef("same"), block.getBytesRef(block.getFirstValueIndex(i), scratch));
                    }
                }
                page.releaseBlocks();
            }
            assertEquals(rows, seen);
        }
    }

    /**
     * Repeating numeric leaf below per-row ancestor nulls. Covers the
     * {@code ColumnBlockConversions.longColumn} path that iterates {@code values[0..rowCount-1]}
     * in its non-repeating branch — without value-array expansion, positions {@code >0} would
     * read stale slots under ORC's repeating contract.
     */
    public void testReadNestedStructAncestorNullPropagationRepeatingLongChild() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("timestamp_ms", TypeDescription.createLong()));
        int rows = 200;
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = rows;
            var ev = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[0];
            LongColumnVector ts = (LongColumnVector) ev.fields[0];
            ev.noNulls = false;
            for (int i = 0; i < rows; i++) {
                ev.isNull[i] = (i % 3 == 0);
                ts.vector[i] = 1_700_000_000L; // identical at every slot
            }
        });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        try (CloseableIterator<Page> iter = reader.read(createStorageObject(orcData), List.of("event.timestamp_ms"), 1024)) {
            int seen = 0;
            while (iter.hasNext()) {
                Page page = iter.next();
                LongBlock block = (LongBlock) page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++, seen++) {
                    if (seen % 3 == 0) {
                        assertTrue("row " + seen + " parent null -> leaf null", block.isNull(i));
                    } else {
                        assertFalse("row " + seen + " parent non-null -> leaf non-null", block.isNull(i));
                        assertEquals(1_700_000_000L, block.getLong(block.getFirstValueIndex(i)));
                    }
                }
                page.releaseBlocks();
            }
            assertEquals(rows, seen);
        }
    }

    public void testNestedStructFilterPushdownPrunesStripes() throws Exception {
        // Two stripes with disjoint event.id ranges. Pushing event.id > 999 must prune the
        // first stripe via SearchArgument-based stripe statistics; the surviving row count
        // must be strictly less than the total.
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("id", TypeDescription.createLong()));

        byte[] orcData = createMultiStripeOrcFile(schema, 2, stripe -> {
            VectorizedRowBatch batch = schema.createRowBatch(100);
            batch.size = 100;
            long base = stripe == 0 ? 0L : 1000L;
            org.apache.hadoop.hive.ql.exec.vector.StructColumnVector ev =
                (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[0];
            LongColumnVector evId = (LongColumnVector) ev.fields[0];
            for (int i = 0; i < 100; i++) {
                evId.vector[i] = base + i;
            }
            return batch;
        });

        // Sanity: without pushdown, all 200 rows survive.
        OrcFormatReader baseline = new OrcFormatReader(blockFactory);
        StorageObject storageObject = createStorageObject(orcData);
        int totalRows = countRows(baseline, storageObject, List.of("event.id"), 1024);
        assertEquals(200, totalRows);

        // Push event.id > 999 — first stripe (max=99) is fully prunable.
        org.elasticsearch.xpack.esql.core.expression.Expression filter =
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan(
                org.elasticsearch.xpack.esql.core.tree.Source.EMPTY,
                new org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute(
                    org.elasticsearch.xpack.esql.core.tree.Source.EMPTY,
                    "event.id",
                    DataType.LONG
                ),
                new org.elasticsearch.xpack.esql.core.expression.Literal(
                    org.elasticsearch.xpack.esql.core.tree.Source.EMPTY,
                    999L,
                    DataType.LONG
                )
            );
        OrcPushedExpressions pushed = new OrcPushedExpressions(List.of(filter));
        OrcFormatReader filtered = (OrcFormatReader) new OrcFormatReader(blockFactory).withPushedFilter(pushed);
        int survived = countRows(filtered, storageObject, List.of("event.id"), 1024);
        assertTrue("expected pruning to drop the first stripe (100 rows). survived=" + survived, survived <= 100 && survived < totalRows);
    }

    public void testNestedColumnStatistics() throws Exception {
        // Two stripes with disjoint event.id ranges; assert per-leaf stats are published with
        // the dotted name, matching the ESQL attribute names the planner sees. Mirrors the
        // Parquet-side testNestedColumnStatistics.
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField(
                "event",
                TypeDescription.createStruct()
                    .addField("id", TypeDescription.createLong())
                    .addField("action", TypeDescription.createString())
            );
        byte[] orcData = createMultiStripeOrcFile(schema, 2, stripe -> {
            VectorizedRowBatch batch = schema.createRowBatch(50);
            batch.size = 50;
            long base = stripe == 0 ? 0L : 1000L;
            String action = stripe == 0 ? "login" : "logout";
            byte[] actionBytes = action.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            LongColumnVector topId = (LongColumnVector) batch.cols[0];
            org.apache.hadoop.hive.ql.exec.vector.StructColumnVector ev =
                (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[1];
            LongColumnVector evId = (LongColumnVector) ev.fields[0];
            BytesColumnVector evAction = (BytesColumnVector) ev.fields[1];
            for (int i = 0; i < 50; i++) {
                topId.vector[i] = base + i;
                evId.vector[i] = base + i;
                evAction.setVal(i, actionBytes);
            }
            return batch;
        });

        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(createStorageObject(orcData));
        assertTrue("expected source statistics", metadata.statistics().isPresent());
        var optColStats = metadata.statistics().get().columnStatistics();
        assertTrue("expected per-column statistics", optColStats.isPresent());
        var colStats = optColStats.get();

        // Nested leaves present at dotted names.
        assertTrue("event.id stats must be published", colStats.containsKey("event.id"));
        assertTrue("event.action stats must be published", colStats.containsKey("event.action"));
        // Top-level still present (no regression).
        assertTrue("top-level id stats still present", colStats.containsKey("id"));
        // STRUCT parent should NOT publish stats — collectStatistics descends into STRUCT
        // children without adding the parent path itself.
        assertFalse("STRUCT parent must not appear as a stats column", colStats.containsKey("event"));

        // event.id ranges across the two stripes are aggregated by ORC into [0, 1049].
        var evIdStats = colStats.get("event.id");
        assertEquals(java.util.Optional.of(0L), evIdStats.minValue());
        assertEquals(java.util.Optional.of(1049L), evIdStats.maxValue());
        assertEquals(java.util.OptionalLong.of(0L), evIdStats.nullCount());

        // event.action min/max are STRING; just assert presence rather than exact value.
        var evActionStats = colStats.get("event.action");
        assertTrue("event.action min must be present", evActionStats.minValue().isPresent());
        assertTrue("event.action max must be present", evActionStats.maxValue().isPresent());

        // Top-level id covers the same range.
        var topIdStats = colStats.get("id");
        assertEquals(java.util.Optional.of(0L), topIdStats.minValue());
        assertEquals(java.util.Optional.of(1049L), topIdStats.maxValue());
    }

    public void testReadNestedStructAncestorNullPropagation() throws Exception {
        // Rows: (a) parent struct null, (b) parent non-null + child null, (c) both non-null.
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("action", TypeDescription.createString()));
        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 3;
            var ev = (org.apache.hadoop.hive.ql.exec.vector.StructColumnVector) batch.cols[0];
            BytesColumnVector action = (BytesColumnVector) ev.fields[0];
            // (a) parent null at row 0
            ev.noNulls = false;
            ev.isNull[0] = true;
            // stale child value at the same slot — must NOT leak through
            action.setVal(0, "stale".getBytes(StandardCharsets.UTF_8));
            // (b) parent non-null, child null
            ev.isNull[1] = false;
            action.noNulls = false;
            action.isNull[1] = true;
            // (c) both non-null
            ev.isNull[2] = false;
            action.isNull[2] = false;
            action.setVal(2, "real".getBytes(StandardCharsets.UTF_8));
        });
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        try (CloseableIterator<Page> iter = reader.read(createStorageObject(orcData), List.of("event.action"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertTrue("row 0 (parent null) -> child null", block.isNull(0));
            assertTrue("row 1 (parent non-null, child null) -> child null", block.isNull(1));
            assertFalse("row 2 (both non-null) -> non-null", block.isNull(2));
            assertEquals(new BytesRef("real"), block.getBytesRef(block.getFirstValueIndex(2), new BytesRef()));
            page.releaseBlocks();
        }
    }

    private StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.orc");
            }
        };
    }
}
