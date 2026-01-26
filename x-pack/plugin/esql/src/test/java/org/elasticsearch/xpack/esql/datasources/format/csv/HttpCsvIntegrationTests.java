/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.format.csv;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.http.HttpConfiguration;
import org.elasticsearch.xpack.esql.datasources.http.HttpStorageProvider;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Integration test for CSV over HTTP.
 * 
 * Tests the complete flow: HTTP storage provider -> CSV format reader -> ESQL Pages
 * 
 * Uses IcebergS3HttpFixture to serve CSV files over plain HTTP (without AWS authentication).
 * The fixture supports both S3-signed requests (for Iceberg/Parquet tests) and plain HTTP requests.
 * 
 * Note: HttpClient creates background selector threads that cannot be easily shut down.
 * These are daemon threads and don't prevent JVM shutdown, so we suppress thread leak warnings.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class HttpCsvIntegrationTests extends ESTestCase {
    
    @ClassRule
    public static S3FixtureUtils.IcebergS3HttpFixture s3Fixture = new S3FixtureUtils.IcebergS3HttpFixture();
    
    private BlockFactory blockFactory;
    private ExecutorService executor;
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.getInstance(
            new NoopCircuitBreaker("test-noop"),
            BigArrays.NON_RECYCLING_INSTANCE
        );
        executor = Executors.newFixedThreadPool(2);
    }
    
    @Override
    public void tearDown() throws Exception {
        executor.shutdown();
        super.tearDown();
    }
    
    public void testReadCsvOverHttp() throws Exception {
        // Sample CSV data
        String csvData = """
            id:long,name:keyword,score:double
            1,Alice,95.5
            2,Bob,87.3
            3,Charlie,92.1
            """;
        
        // Add CSV to fixture
        s3Fixture.addContent("/test/data.csv", csvData);
        
        // Create HTTP storage provider
        HttpConfiguration config = HttpConfiguration.builder()
            .connectTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofSeconds(30))
            .build();
        
        try (HttpStorageProvider storageProvider = new HttpStorageProvider(config, executor)) {
            // Create storage path using fixture endpoint
            String endpoint = s3Fixture.getAddress();
            StoragePath path = StoragePath.of(endpoint + "/test/data.csv");
            StorageObject storageObject = storageProvider.newObject(path);
            
            // Create CSV format reader
            CsvFormatReader formatReader = new CsvFormatReader(blockFactory);
            
            // Test schema reading
            List<Attribute> schema = formatReader.getSchema(storageObject);
            assertEquals(3, schema.size());
            assertEquals("id", schema.get(0).name());
            assertEquals("name", schema.get(1).name());
            assertEquals("score", schema.get(2).name());
            
            // Test data reading
            try (CloseableIterator<Page> iterator = formatReader.read(storageObject, null, 10)) {
                assertTrue(iterator.hasNext());
                Page page = iterator.next();
                
                assertEquals(3, page.getPositionCount());
                assertEquals(3, page.getBlockCount());
                
                // Verify data
                LongBlock idBlock = (LongBlock) page.getBlock(0);
                BytesRefBlock nameBlock = (BytesRefBlock) page.getBlock(1);
                DoubleBlock scoreBlock = (DoubleBlock) page.getBlock(2);
                
                assertEquals(1L, idBlock.getLong(0));
                assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
                assertEquals(95.5, scoreBlock.getDouble(0), 0.001);
                
                assertEquals(2L, idBlock.getLong(1));
                assertEquals(new BytesRef("Bob"), nameBlock.getBytesRef(1, new BytesRef()));
                assertEquals(87.3, scoreBlock.getDouble(1), 0.001);
                
                assertEquals(3L, idBlock.getLong(2));
                assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(2, new BytesRef()));
                assertEquals(92.1, scoreBlock.getDouble(2), 0.001);
                
                assertFalse(iterator.hasNext());
            }
        }
    }
    
    public void testReadCsvWithProjection() throws Exception {
        String csvData = """
            id:long,name:keyword,age:integer,score:double
            1,Alice,30,95.5
            2,Bob,25,87.3
            """;
        
        // Add CSV to fixture
        s3Fixture.addContent("/test/projection.csv", csvData);
        
        HttpConfiguration config = HttpConfiguration.builder()
            .connectTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofSeconds(30))
            .build();
        
        try (HttpStorageProvider storageProvider = new HttpStorageProvider(config, executor)) {
            String endpoint = s3Fixture.getAddress();
            StoragePath path = StoragePath.of(endpoint + "/test/projection.csv");
            StorageObject storageObject = storageProvider.newObject(path);
            
            CsvFormatReader formatReader = new CsvFormatReader(blockFactory);
            
            // Read only name and score columns
            try (CloseableIterator<Page> iterator = formatReader.read(
                storageObject,
                List.of("name", "score"),
                10
            )) {
                assertTrue(iterator.hasNext());
                Page page = iterator.next();
                
                assertEquals(2, page.getPositionCount());
                assertEquals(2, page.getBlockCount()); // Only projected columns
                
                BytesRefBlock nameBlock = (BytesRefBlock) page.getBlock(0);
                DoubleBlock scoreBlock = (DoubleBlock) page.getBlock(1);
                
                assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
                assertEquals(95.5, scoreBlock.getDouble(0), 0.001);
                
                assertEquals(new BytesRef("Bob"), nameBlock.getBytesRef(1, new BytesRef()));
                assertEquals(87.3, scoreBlock.getDouble(1), 0.001);
            }
        }
    }
    
    public void testReadLargeCsvWithBatching() throws Exception {
        // Generate CSV with 25 rows
        StringBuilder csvData = new StringBuilder("id:long,value:integer\n");
        for (int i = 1; i <= 25; i++) {
            csvData.append(i).append(",").append(i * 10).append("\n");
        }
        
        // Add CSV to fixture
        s3Fixture.addContent("/test/large.csv", csvData.toString());
        
        HttpConfiguration config = HttpConfiguration.builder()
            .connectTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofSeconds(30))
            .build();
        
        try (HttpStorageProvider storageProvider = new HttpStorageProvider(config, executor)) {
            String endpoint = s3Fixture.getAddress();
            StoragePath path = StoragePath.of(endpoint + "/test/large.csv");
            StorageObject storageObject = storageProvider.newObject(path);
            
            CsvFormatReader formatReader = new CsvFormatReader(blockFactory);
            
            int batchSize = 10;
            int totalRows = 0;
            
            try (CloseableIterator<Page> iterator = formatReader.read(storageObject, null, batchSize)) {
                // First batch: 10 rows
                assertTrue(iterator.hasNext());
                Page page1 = iterator.next();
                assertEquals(10, page1.getPositionCount());
                totalRows += page1.getPositionCount();
                
                // Second batch: 10 rows
                assertTrue(iterator.hasNext());
                Page page2 = iterator.next();
                assertEquals(10, page2.getPositionCount());
                totalRows += page2.getPositionCount();
                
                // Third batch: 5 rows
                assertTrue(iterator.hasNext());
                Page page3 = iterator.next();
                assertEquals(5, page3.getPositionCount());
                totalRows += page3.getPositionCount();
                
                assertFalse(iterator.hasNext());
            }
            
            assertEquals(25, totalRows);
        }
    }
    
    public void testHttpServerNotFound() throws Exception {
        HttpConfiguration config = HttpConfiguration.builder()
            .connectTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofSeconds(30))
            .build();
        
        try (HttpStorageProvider storageProvider = new HttpStorageProvider(config, executor)) {
            // Use a port that's likely not in use
            StoragePath path = StoragePath.of("http://localhost:59999/nonexistent.csv");
            StorageObject storageObject = storageProvider.newObject(path);
            
            CsvFormatReader formatReader = new CsvFormatReader(blockFactory);
            
            // Should throw IOException when trying to read from non-existent server
            expectThrows(IOException.class, () -> formatReader.getSchema(storageObject));
        }
    }
}
