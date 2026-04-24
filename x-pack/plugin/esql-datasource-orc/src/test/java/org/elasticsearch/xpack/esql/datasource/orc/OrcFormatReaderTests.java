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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

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

public class OrcFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
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
                projection,
                1024,
                range.offset(),
                range.offset() + range.length(),
                List.of(),
                null
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
                null,
                1024,
                range.offset(),
                range.offset() + range.length(),
                List.of(),
                null
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
                null,
                1024,
                range.offset(),
                range.offset() + range.length(),
                List.of(),
                null
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

    // --- ORC file creation helpers ---

    @FunctionalInterface
    private interface StripeBatchProducer {
        VectorizedRowBatch produce(int stripeIndex) throws IOException;
    }

    @FunctionalInterface
    private interface BatchPopulator {
        void populate(VectorizedRowBatch batch);
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
