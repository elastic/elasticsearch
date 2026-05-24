/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class TemplateGeneratorTests extends ESTestCase {
    public void testSanity() {
        var specification = DataGeneratorSpecification.buildDefault();
        var generator = new TemplateGenerator(specification);

        var template = generator.generate();
        validateMappingTemplate(template.template());
    }

    private void validateMappingTemplate(Map<String, Template.Entry> template) {
        // Just a high level sanity check, we test that mapping and documents make sense in DataGenerationTests.
        for (var entry : template.entrySet()) {
            assertNotNull(entry.getKey());
            assertFalse(entry.getKey().isEmpty());
            switch (entry.getValue()) {
                case Template.Leaf leaf -> {
                    assertEquals(entry.getKey(), leaf.name());
                    assertNotNull(leaf.type());
                }
                case Template.Object object -> {
                    assertEquals(entry.getKey(), object.name());
                    assertNotNull(object.children());
                    validateMappingTemplate(object.children());
                }
            }
        }
    }
}
