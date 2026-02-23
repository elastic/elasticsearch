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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.lucene.util.BytesRef;
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
