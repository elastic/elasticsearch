/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

public class DocumentGeneratorTests extends ESTestCase {
    public void testSanity() throws IOException {
        var specification = DataGeneratorSpecification.buildDefault();

        var templateGenerator = new TemplateGenerator(specification);
        var template = templateGenerator.generate();

        var mappingGenerator = new MappingGenerator(specification);
        var mapping = mappingGenerator.generate(template, null);

        var generator = new DocumentGenerator(specification);
        var document = generator.generate(template, mapping);
        validateDocument(document);

        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(document);
        builder.flush();
    }

    private void validateDocument(Map<String, Object> mapping) {

    }
}
