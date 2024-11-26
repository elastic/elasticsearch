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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class TemplateGenerator {
    private final DataGeneratorSpecification specification;
    private final DataSourceResponse.ChildFieldGenerator childFieldGenerator;
    private final Supplier<DataSourceResponse.FieldTypeGenerator.FieldTypeInfo> fieldTypeGenerator;

    public TemplateGenerator(DataGeneratorSpecification specification) {
        this.specification = specification;
        this.childFieldGenerator = specification.dataSource().get(new DataSourceRequest.ChildFieldGenerator(specification));
        this.fieldTypeGenerator = specification.dataSource().get(new DataSourceRequest.FieldTypeGenerator()).generator();
    }

    public Template generate() {
        var map = new HashMap<String, Template.Entry>();

        generateChildFields(map, 0, new AtomicInteger(0));
        return new Template(map);
    }

    private void generateChildFields(Map<String, Template.Entry> mapping, int depth, AtomicInteger nestedFieldsCount) {
        var existingFieldNames = new HashSet<String>();
        // no child fields is legal
        var childFieldsCount = childFieldGenerator.generateChildFieldCount();

        for (int i = 0; i < childFieldsCount; i++) {
            var fieldName = generateFieldName(existingFieldNames);

            if (depth < specification.maxObjectDepth() && childFieldGenerator.generateRegularSubObject()) {
                var children = new HashMap<String, Template.Entry>();
                mapping.put(fieldName, new Template.Object(fieldName, false, children));
                generateChildFields(children, depth + 1, nestedFieldsCount);
            } else if (depth <= specification.maxObjectDepth()
                && nestedFieldsCount.get() < specification.nestedFieldsLimit()
                && childFieldGenerator.generateNestedSubObject()) {
                    nestedFieldsCount.incrementAndGet();

                    var children = new HashMap<String, Template.Entry>();
                    mapping.put(fieldName, new Template.Object(fieldName, true, children));
                    generateChildFields(children, depth + 1, nestedFieldsCount);
                } else {
                    var fieldTypeInfo = fieldTypeGenerator.get();
                    mapping.put(fieldName, new Template.Leaf(fieldName, fieldTypeInfo.fieldType()));
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
