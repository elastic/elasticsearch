/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

class Context {
    private final DataGeneratorSpecification specification;

    private final DataSourceResponse.ChildFieldGenerator childFieldGenerator;
    private final DataSourceResponse.ObjectArrayGenerator objectArrayGenerator;
    private final int objectDepth;
    // We don't need atomicity, but we need to pass counter by reference to accumulate total value from sub-objects.
    private final AtomicInteger nestedFieldsCount;
    private final DynamicMapping parentDynamicMapping;

    Context(DataGeneratorSpecification specification, DynamicMapping parentDynamicMapping) {
        this(specification, 0, new AtomicInteger(0), parentDynamicMapping);
    }

    private Context(
        DataGeneratorSpecification specification,
        int objectDepth,
        AtomicInteger nestedFieldsCount,
        DynamicMapping parentDynamicMapping
    ) {
        this.specification = specification;
        this.childFieldGenerator = specification.dataSource().get(new DataSourceRequest.ChildFieldGenerator(specification));
        this.objectArrayGenerator = specification.dataSource().get(new DataSourceRequest.ObjectArrayGenerator());
        this.objectDepth = objectDepth;
        this.nestedFieldsCount = nestedFieldsCount;
        this.parentDynamicMapping = parentDynamicMapping;
    }

    public DataGeneratorSpecification specification() {
        return specification;
    }

    public DataSourceResponse.ChildFieldGenerator childFieldGenerator() {
        return childFieldGenerator;
    }

    public DataSourceResponse.FieldTypeGenerator fieldTypeGenerator(DynamicMapping dynamicMapping) {
        return specification.dataSource().get(new DataSourceRequest.FieldTypeGenerator(dynamicMapping));
    }

    public Context subObject(DynamicMapping dynamicMapping) {
        return new Context(specification, objectDepth + 1, nestedFieldsCount, dynamicMapping);
    }

    public Context nestedObject(DynamicMapping dynamicMapping) {
        nestedFieldsCount.incrementAndGet();
        return new Context(specification, objectDepth + 1, nestedFieldsCount, dynamicMapping);
    }

    public boolean shouldAddDynamicObjectField(DynamicMapping dynamicMapping) {
        if (objectDepth >= specification.maxObjectDepth() || dynamicMapping == DynamicMapping.FORBIDDEN) {
            return false;
        }

        return childFieldGenerator.generateDynamicSubObject();
    }

    public boolean shouldAddObjectField() {
        if (objectDepth >= specification.maxObjectDepth() || parentDynamicMapping == DynamicMapping.FORCED) {
            return false;
        }

        return childFieldGenerator.generateRegularSubObject();
    }

    public boolean shouldAddNestedField() {
        if (objectDepth >= specification.maxObjectDepth() || nestedFieldsCount.get() >= specification.nestedFieldsLimit() || parentDynamicMapping == DynamicMapping.FORCED) {
            return false;
        }

        return childFieldGenerator.generateNestedSubObject();
    }

    public Optional<Integer> generateObjectArray() {
        if (objectDepth == 0) {
            return Optional.empty();
        }

        return objectArrayGenerator.lengthGenerator().get();
    }

    public DynamicMapping determineDynamicMapping(Map<String, Object> mappingParameters) {
        if (parentDynamicMapping == DynamicMapping.FORCED) {
            return DynamicMapping.FORCED;
        }

        var dynamicParameter = mappingParameters.get("dynamic");
        // Inherited from parent
        if (dynamicParameter == null) {
            return parentDynamicMapping;
        }

        return dynamicParameter.equals("strict") ? DynamicMapping.FORBIDDEN : DynamicMapping.SUPPORTED;
    }
}
