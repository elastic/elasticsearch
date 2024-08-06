/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class DataGeneratorTests extends ESTestCase {
    public void testDataGeneratorSanity() throws IOException {
        var dataGenerator = new DataGenerator(DataGeneratorSpecification.buildDefault());

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.writeMapping(mapping);

        for (int i = 0; i < 1000; i++) {
            var document = XContentBuilder.builder(XContentType.JSON.xContent());
            dataGenerator.generateDocument(document);
        }
    }

    public void testDataGeneratorProducesValidMappingAndDocument() throws IOException {
        // Make sure objects, nested objects and all field types are covered.
        var testChildFieldGenerator = new DataSourceResponse.ChildFieldGenerator() {
            private boolean subObjectCovered = false;
            private boolean nestedCovered = false;
            private int generatedFields = 0;

            @Override
            public int generateChildFieldCount() {
                // Make sure to generate enough fields to go through all field types.
                return 20;
            }

            @Override
            public boolean generateNestedSubObject() {
                if (nestedCovered == false) {
                    nestedCovered = true;
                    return true;
                }

                return false;
            }

            @Override
            public boolean generateRegularSubObject() {
                if (subObjectCovered == false) {
                    subObjectCovered = true;
                    return true;
                }

                return false;
            }

            @Override
            public String generateFieldName() {
                return "f" + generatedFields++;
            }
        };

        var dataSourceOverride = new DataSourceHandler() {
            private int generatedFields = 0;

            @Override
            public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
                return testChildFieldGenerator;
            }

            @Override
            public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
                return new DataSourceResponse.FieldTypeGenerator(() -> FieldType.values()[generatedFields++ % FieldType.values().length]);
            }
        };

        var dataGenerator = new DataGenerator(
            DataGeneratorSpecification.builder().withDataSourceHandlers(List.of(dataSourceOverride)).build()
        );

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.writeMapping(mapping);

        var mappingService = new MapperServiceTestCase() {
            @Override
            protected Collection<? extends Plugin> getPlugins() {
                return List.of(new UnsignedLongMapperPlugin(), new MapperExtrasPlugin());
            }
        }.createMapperService(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.generateDocument(document);

        mappingService.documentMapper().parse(new SourceToParse("1", BytesReference.bytes(document), XContentType.JSON));
    }

    public void testDataGeneratorStressTest() throws IOException {
        // Let's generate 1000000 fields to test an extreme case (2 levels of objects + 1 leaf level with 100 fields per object).
        var testChildFieldGenerator = new DataSourceResponse.ChildFieldGenerator() {
            private int generatedFields = 0;

            @Override
            public int generateChildFieldCount() {
                return 100;
            }

            @Override
            public boolean generateNestedSubObject() {
                return false;
            }

            @Override
            public boolean generateRegularSubObject() {
                return true;
            }

            @Override
            public String generateFieldName() {
                return "f" + generatedFields++;
            }
        };

        var dataSourceOverride = new DataSourceHandler() {
            @Override
            public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
                return testChildFieldGenerator;
            }

            @Override
            public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
                return new DataSourceResponse.ObjectArrayGenerator(Optional::empty);
            }

            @Override
            public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
                return new DataSourceResponse.FieldTypeGenerator(() -> FieldType.LONG);
            }
        };

        var dataGenerator = new DataGenerator(
            DataGeneratorSpecification.builder().withDataSourceHandlers(List.of(dataSourceOverride)).withMaxObjectDepth(2).build()
        );

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.writeMapping(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.generateDocument(document);
    }
}
