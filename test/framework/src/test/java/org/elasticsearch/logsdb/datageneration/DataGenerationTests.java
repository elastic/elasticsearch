/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.countedkeyword.CountedKeywordMapperPlugin;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class DataGenerationTests extends ESTestCase {
    public void testDataGenerationProducesValidMappingAndDocument() throws IOException {
        // Make sure objects, nested objects and all field types are covered.
        var testChildFieldGenerator = new DataSourceResponse.ChildFieldGenerator() {
            private boolean dynamicSubObjectCovered = false;
            private boolean subObjectCovered = false;
            private boolean nestedCovered = false;
            private int generatedFields = 0;

            @Override
            public int generateChildFieldCount() {
                // Make sure to generate enough fields to go through all field types.
                return 20;
            }

            @Override
            public boolean generateDynamicSubObject() {
                if (dynamicSubObjectCovered == false) {
                    dynamicSubObjectCovered = true;
                    return true;
                }

                return false;
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
                return new DataSourceResponse.FieldTypeGenerator(
                    () -> new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(
                        FieldType.values()[generatedFields++ % FieldType.values().length].toString()
                    )
                );

            }
        };

        var specification = DataGeneratorSpecification.builder().withDataSourceHandlers(List.of(dataSourceOverride)).build();

        var documentGenerator = new DocumentGenerator(specification);

        var template = new TemplateGenerator(specification).generate();
        var mapping = new MappingGenerator(specification).generate(template);

        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent());
        mappingXContent.map(mapping.raw());

        var mappingService = new MapperServiceTestCase() {
            @Override
            protected Collection<? extends Plugin> getPlugins() {
                return List.of(
                    new UnsignedLongMapperPlugin(),
                    new MapperExtrasPlugin(),
                    new CountedKeywordMapperPlugin(),
                    new ConstantKeywordMapperPlugin(),
                    new Wildcard()
                );
            }
        }.createMapperService(mappingXContent);

        var document = XContentBuilder.builder(XContentType.JSON.xContent());
        document.map(documentGenerator.generate(template, mapping));

        mappingService.documentMapper().parse(new SourceToParse("1", BytesReference.bytes(document), XContentType.JSON));
    }

    public void testDataGeneratorStressTest() throws IOException {
        // Let's generate 125000 fields to test an extreme case (2 levels of objects + 1 leaf level with 50 fields per object).
        var testChildFieldGenerator = new DataSourceResponse.ChildFieldGenerator() {
            private int generatedFields = 0;

            @Override
            public int generateChildFieldCount() {
                return 50;
            }

            @Override
            public boolean generateDynamicSubObject() {
                return false;
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
                return new DataSourceResponse.FieldTypeGenerator(
                    () -> new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(FieldType.LONG.toString())
                );
            }
        };
        var specification = DataGeneratorSpecification.builder()
            .withDataSourceHandlers(List.of(dataSourceOverride))
            .withMaxObjectDepth(2)
            .build();

        var template = new TemplateGenerator(specification).generate();
        var mapping = new MappingGenerator(specification).generate(template);
        var ignored = new DocumentGenerator(specification).generate(template, mapping);
    }
}
