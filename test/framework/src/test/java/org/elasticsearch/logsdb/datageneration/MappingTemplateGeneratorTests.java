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

public class MappingTemplateGeneratorTests extends ESTestCase {
    public void testSanity() {
        var specification = DataGeneratorSpecification.buildDefault();
        var generator = new MappingTemplateGenerator(specification);

        var template = generator.generate();
        validateMappingTemplate(template.mapping());
    }

    private void validateMappingTemplate(Map<String, MappingTemplate.Entry> template) {
        // Just a high level sanity check, can be more involved
        for (var entry : template.entrySet()) {
            assertNotNull(entry.getKey());
            assertFalse(entry.getKey().isEmpty());
            switch (entry.getValue()) {
                case MappingTemplate.Entry.Leaf leaf -> {
                    assertEquals(entry.getKey(), leaf.name());
                    assertNotNull(leaf.type());
                }
                case MappingTemplate.Entry.Object object -> {
                    assertEquals(entry.getKey(), object.name());
                    assertNotNull(object.children());
                    validateMappingTemplate(object.children());
                }
            }
        }
    }
}
