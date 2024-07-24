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
import org.elasticsearch.logsdb.datageneration.arbitrary.Arbitrary;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

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
        var testArbitrary = new Arbitrary() {
            private boolean subObjectCovered = false;
            private boolean nestedCovered = false;
            private int generatedFields = 0;

            @Override
            public boolean generateSubObject() {
                if (subObjectCovered == false) {
                    subObjectCovered = true;
                    return true;
                }

                return false;
            }

            @Override
            public boolean generateNestedObject() {
                if (nestedCovered == false) {
                    nestedCovered = true;
                    return true;
                }

                return false;
            }

            @Override
            public int childFieldCount(int lowerBound, int upperBound) {
                // Make sure to generate enough fields to go through all field types.
                return 20;
            }

            @Override
            public String fieldName(int lengthLowerBound, int lengthUpperBound) {
                return "f" + generatedFields++;
            }

            @Override
            public FieldType fieldType() {
                return FieldType.values()[generatedFields % FieldType.values().length];
            }

            @Override
            public long longValue() {
                return randomLong();
            }

            @Override
            public String stringValue(int lengthLowerBound, int lengthUpperBound) {
                return randomAlphaOfLengthBetween(lengthLowerBound, lengthUpperBound);
            }
        };

        var dataGenerator = new DataGenerator(DataGeneratorSpecification.builder().withArbitrary(testArbitrary).build());

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.writeMapping(mapping);

        var mappingService = new MapperServiceTestCase() {
        }.createMapperService(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.generateDocument(document);

        mappingService.documentMapper().parse(new SourceToParse("1", BytesReference.bytes(document), XContentType.JSON));
    }

    public void testDataGeneratorStressTest() throws IOException {
        // Let's generate 1000000 fields to test an extreme case (2 levels of objects + 1 leaf level with 100 fields per object).
        var arbitrary = new Arbitrary() {
            private int generatedFields = 0;

            @Override
            public boolean generateSubObject() {
                return true;
            }

            @Override
            public boolean generateNestedObject() {
                return false;
            }

            @Override
            public int childFieldCount(int lowerBound, int upperBound) {
                return upperBound;
            }

            @Override
            public String fieldName(int lengthLowerBound, int lengthUpperBound) {
                return "f" + generatedFields++;
            }

            @Override
            public FieldType fieldType() {
                return FieldType.LONG;
            }

            @Override
            public long longValue() {
                return 0;
            }

            @Override
            public String stringValue(int lengthLowerBound, int lengthUpperBound) {
                return "";
            }
        };
        var dataGenerator = new DataGenerator(
            DataGeneratorSpecification.builder().withArbitrary(arbitrary).withMaxFieldCountPerLevel(100).withMaxObjectDepth(2).build()
        );

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.writeMapping(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.generateDocument(document);
    }
}
