/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.logsdb.datageneration.fields.DynamicMapping;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class MappingTemplateGenerator {
    private final DataGeneratorSpecification specification;
    private final DataSourceResponse.ChildFieldGenerator childFieldGenerator;
    private final Supplier<DataSourceResponse.FieldTypeGenerator.FieldTypeInfo> fieldTypeGenerator;

    public MappingTemplateGenerator(DataGeneratorSpecification specification) {
        this.specification = specification;
        this.childFieldGenerator = specification.dataSource().get(new DataSourceRequest.ChildFieldGenerator(specification));
        // TODO DynamicMapping should not be here, should be in mapping generation instead?
        this.fieldTypeGenerator = specification.dataSource()
            .get(new DataSourceRequest.FieldTypeGenerator(DynamicMapping.FORBIDDEN))
            .generator();
    }

    // TODO enforce nested fields limit (easy)
    public MappingTemplate generate() {
        var map = new HashMap<String, MappingTemplate.Entry>();

        generateChildFields(map, 0);
        return new MappingTemplate(map);
    }

    private void generateChildFields(Map<String, MappingTemplate.Entry> mapping, int depth, AtomicInteger nestedFieldsCount) {
        var existingFieldNames = new HashSet<String>();
        // no child fields is legal
        var childFieldsCount = childFieldGenerator.generateChildFieldCount();

        for (int i = 0; i < childFieldsCount; i++) {
            var fieldName = generateFieldName(existingFieldNames);

            if (depth <= specification.maxObjectDepth() && childFieldGenerator.generateRegularSubObject()) {
                var children = new HashMap<String, MappingTemplate.Entry>();
                mapping.put(fieldName, new MappingTemplate.Entry.Object(fieldName, false, children));
                generateChildFields(children, depth + 1, nestedFieldsCount);
            } else if (depth <= specification.maxObjectDepth()
                && nestedFieldsCount.get() < specification.nestedFieldsLimit()
                && childFieldGenerator.generateNestedSubObject()) {
                    nestedFieldsCount.incrementAndGet();

                    var children = new HashMap<String, MappingTemplate.Entry>();
                    mapping.put(fieldName, new MappingTemplate.Entry.Object(fieldName, true, children));
                    generateChildFields(children, depth + 1, nestedFieldsCount);
                } else {
                    var fieldTypeInfo = fieldTypeGenerator.get();
                    mapping.put(fieldName, new MappingTemplate.Entry.Leaf(fieldName, fieldTypeInfo.fieldType()));
                }
        }
    }

    private String generateFieldName(Set<String> existingFields) {
        var fieldName = childFieldGenerator.generateFieldName();
        while (existingFields.contains(fieldName)) {
            fieldName = childFieldGenerator.generateFieldName();
        }
        existingFields.add(fieldName);

        return fieldName;
    }
}
