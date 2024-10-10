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

import java.util.Map;

public class MappingGeneratorTests extends ESTestCase {
    public void testSanity() {
        var specification = DataGeneratorSpecification.buildDefault();

        var templateGenerator = new TemplateGenerator(specification);
        var template = templateGenerator.generate();

        var generator = new MappingGenerator(specification);
        var mapping = generator.generate(template, null);
        validateMapping(mapping.raw());
    }

    private void validateMapping(Map<String, Object> mapping) {

    }
}
