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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class DataGeneratorTests extends ESTestCase {
    public void testDataGeneratorSanity() throws IOException {
        var dataGenerator = new DataGenerator(new DataGeneratorSpecification());

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.writeMapping(mapping);

        for (int i = 0; i < 1000; i++) {
            var document = XContentBuilder.builder(XContentType.JSON.xContent());
            dataGenerator.generateDocument(document);
        }
    }

    public void testDataGeneratorProducesValidMappingAndDocument() throws IOException {
        // Let's keep number of fields under 1000 field limit
        var dataGenerator = new DataGenerator(new DataGeneratorSpecification(10, 3));

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.writeMapping(mapping);

        var mappingService = new MapperServiceTestCase() {
        }.createMapperService(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.generateDocument(document);

        mappingService.documentMapper().parse(new SourceToParse("1", BytesReference.bytes(document), XContentType.JSON));
    }

    public void testDataGeneratorStressTest() throws IOException {
        var dataGenerator = new DataGenerator(new DataGeneratorSpecification(300, 3));

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.writeMapping(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent());
        dataGenerator.generateDocument(document);

    }
}
