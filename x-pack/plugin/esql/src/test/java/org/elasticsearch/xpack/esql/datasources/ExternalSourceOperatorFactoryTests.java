/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Tests for ExternalSourceOperatorFactory.
 *
 * This demonstrates the integration of StorageProvider and FormatReader
 * to create source operators for external data.
 */
public class ExternalSourceOperatorFactoryTests extends ESTestCase {

    public void testCreateOperatorWithMockedStorageAndFormat() throws IOException {
        // Create a temporary CSV file
        Path tempFile = createTempFile("test", ".csv");
        String csvContent = """
            name,age,city
            Alice,30,NYC
            Bob,25,LA
            Charlie,35,SF
            """;
        Files.writeString(tempFile, csvContent);

        // Create mock storage provider and format reader
        StorageProvider storageProvider = Mockito.mock(StorageProvider.class);
        Mockito.when(storageProvider.supportedSchemes()).thenReturn(List.of("file"));
        StorageObject storageObject = Mockito.mock(StorageObject.class);
        StoragePath path = StoragePath.of("file://" + tempFile.toAbsolutePath());
        Mockito.when(storageProvider.newObject(Mockito.any(StoragePath.class))).thenReturn(storageObject);

        FormatReader formatReader = Mockito.mock(FormatReader.class);
        Mockito.when(formatReader.formatName()).thenReturn("csv");
        @SuppressWarnings("unchecked")
        CloseableIterator<org.elasticsearch.compute.data.Page> emptyIterator = Mockito.mock(CloseableIterator.class);
        Mockito.when(emptyIterator.hasNext()).thenReturn(false);
        Mockito.when(formatReader.read(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(emptyIterator);

        // Define attributes (schema)
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "name",
                new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "age",
                new EsField("age", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "city",
                new EsField("city", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        // Create operator factory
        ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            1000  // batch size
        );

        // Create a mock driver context
        BlockFactory blockFactory = Mockito.mock(BlockFactory.class);
        DriverContext driverContext = Mockito.mock(DriverContext.class);
        Mockito.when(driverContext.blockFactory()).thenReturn(blockFactory);

        // Create the operator
        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        // Verify the factory description
        String description = factory.describe();
        assertTrue(description.contains("csv"));
        assertTrue(description.contains("file://"));
    }

    public void testFactoryValidation() {
        StorageProvider storageProvider = Mockito.mock(StorageProvider.class);
        FormatReader formatReader = Mockito.mock(FormatReader.class);
        StoragePath path = StoragePath.of("file:///tmp/test.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "name",
                new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        // Test null storage provider
        expectThrows(IllegalArgumentException.class, () -> new ExternalSourceOperatorFactory(null, formatReader, path, attributes, 1000));

        // Test null format reader
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, null, path, attributes, 1000)
        );

        // Test null path
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, formatReader, null, attributes, 1000)
        );

        // Test null attributes
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, formatReader, path, null, 1000)
        );

        // Test invalid batch size
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, 0)
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, -1)
        );
    }

    public void testDescribe() {
        StorageProvider storageProvider = Mockito.mock(StorageProvider.class);
        FormatReader formatReader = Mockito.mock(FormatReader.class);
        Mockito.when(formatReader.formatName()).thenReturn("csv");
        StoragePath path = StoragePath.of("file:///tmp/data.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "col1",
                new EsField("col1", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, 500);

        String description = factory.describe();
        assertTrue(description.contains("ExternalSourceOperator"));
        assertTrue(description.contains("csv"));
        assertTrue(description.contains("file:///tmp/data.csv"));
        assertTrue(description.contains("500"));
    }
}
