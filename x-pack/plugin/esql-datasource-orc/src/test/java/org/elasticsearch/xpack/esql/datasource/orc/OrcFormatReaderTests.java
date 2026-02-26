/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.lucene.util.BytesRef;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
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
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
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
            nameCol.setVal(0, "Alice".getBytes(java.nio.charset.StandardCharsets.UTF_8));
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
            nameCol.setVal(0, "Alice".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            scoreCol.vector[0] = 95.5;

            idCol.vector[1] = 2L;
            nameCol.setVal(1, "Bob".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            scoreCol.vector[1] = 87.3;

            idCol.vector[2] = 3L;
            nameCol.setVal(2, "Charlie".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            scoreCol.vector[2] = 92.1;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

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

            assertFalse(iterator.hasNext());
        }
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
            nameCol.setVal(0, "Alice".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            scoreCol.vector[0] = 95.5;

            idCol.vector[1] = 2L;
            nameCol.setVal(1, "Bob".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            scoreCol.vector[1] = 87.3;
        });

        StorageObject storageObject = createStorageObject(orcData);
        OrcFormatReader reader = new OrcFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("name", "score"), 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.001);

            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(1)).getDouble(1), 0.001);
        }
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("id", "nonexistent", "score"), 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertTrue(page.getBlock(1).isNull(0));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertTrue(page.getBlock(1).isNull(1));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);
        }
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

        int totalRows = 0;
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
            }
        }

        assertEquals(25, totalRows);
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            assertTrue(((BooleanBlock) page.getBlock(1)).getBoolean(0));
            assertFalse(((BooleanBlock) page.getBlock(1)).getBoolean(1));
        }
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());

            assertEquals(100, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(200, ((IntBlock) page.getBlock(0)).getInt(1));
            assertEquals(300, ((IntBlock) page.getBlock(0)).getInt(2));
        }
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            assertEquals(98.6, ((DoubleBlock) page.getBlock(0)).getDouble(0), 0.1);
            assertEquals(37.0, ((DoubleBlock) page.getBlock(0)).getDouble(1), 0.1);
        }
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

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
        }
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(5, page.getPositionCount());

            LongBlock idBlock = (LongBlock) page.getBlock(0);
            for (int i = 0; i < 5; i++) {
                assertEquals(i + 1, idBlock.getLong(i));
            }

            IntBlock constantBlock = (IntBlock) page.getBlock(1);
            for (int i = 0; i < 5; i++) {
                assertEquals(42, constantBlock.getInt(i));
            }
        }
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            LongBlock tsBlock = (LongBlock) page.getBlock(1);
            assertEquals(epochMillis, tsBlock.getLong(0));
            assertEquals(epochMillis + 3600_000, tsBlock.getLong(1));
        }
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            LongBlock dateBlock = (LongBlock) page.getBlock(1);
            long expectedMillis0 = daysSinceEpoch * 86_400_000L;
            long expectedMillis1 = (daysSinceEpoch + 1) * 86_400_000L;
            assertEquals(expectedMillis0, dateBlock.getLong(0));
            assertEquals(expectedMillis1, dateBlock.getLong(1));
        }
    }

    public void testReadUnsupportedTypeReturnsNullBlock() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("tags", TypeDescription.createList(TypeDescription.createString()));

        byte[] orcData = createOrcFile(schema, batch -> {
            batch.size = 2;
            LongColumnVector idCol = (LongColumnVector) batch.cols[0];
            ListColumnVector tagsCol = (ListColumnVector) batch.cols[1];
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
        List<Attribute> attributes = metadata.schema();
        assertEquals(DataType.UNSUPPORTED, attributes.get(1).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1024)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            LongBlock idBlock = (LongBlock) page.getBlock(0);
            assertEquals(1L, idBlock.getLong(0));
            assertEquals(2L, idBlock.getLong(1));

            Block tagsBlock = page.getBlock(1);
            assertTrue(tagsBlock.isNull(0));
            assertTrue(tagsBlock.isNull(1));
        }
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

    @FunctionalInterface
    private interface BatchPopulator {
        void populate(VectorizedRowBatch batch);
    }

    private byte[] createOrcFile(TypeDescription schema, BatchPopulator populator) throws IOException {
        java.nio.file.Path tempFile = createTempFile();
        Files.delete(tempFile);
        Path orcPath = new Path(tempFile.toUri());

        Configuration conf = new Configuration(false);
        // Use in-memory key provider to avoid Hadoop's Shell class initialization, which
        // attempts to start a process (blocked by entitlements as "never entitled")
        conf.set("orc.key.provider", "memory");
        // Use a minimal FileSystem that avoids Hadoop's UserGroupInformation (Subject.getSubject()
        // is unsupported on Java 25+) and doesn't call chmod (blocked by entitlements)
        NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
        localFs.setConf(conf);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(localFs)
            .compress(org.apache.orc.CompressionKind.NONE);

        try (Writer writer = OrcFile.createWriter(orcPath, writerOptions)) {
            VectorizedRowBatch batch = schema.createRowBatch();
            populator.populate(batch);
            writer.addRowBatch(batch);
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
    private static class NoPermissionLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override
        @SuppressForbidden(reason = "Bypass Hadoop's LocalFSFileOutputStream to avoid Shell.<clinit>")
        protected OutputStream createOutputStreamWithMode(Path p, boolean append, org.apache.hadoop.fs.permission.FsPermission permission)
            throws IOException {
            return new FileOutputStream(pathToFile(p), append);
        }

        @Override
        public void setPermission(Path p, org.apache.hadoop.fs.permission.FsPermission permission) {
            // no-op: skip chmod calls that would trigger Shell
        }

        @Override
        @SuppressForbidden(reason = "Hadoop API requires java.io.File in method signature")
        protected boolean mkOneDirWithMode(Path p, java.io.File p2f, org.apache.hadoop.fs.permission.FsPermission permission)
            throws IOException {
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
