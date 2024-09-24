/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class Context {
    private final DataGeneratorSpecification specification;

    private final DataSourceResponse.ChildFieldGenerator childFieldGenerator;
    private final DataSourceResponse.ObjectArrayGenerator objectArrayGenerator;

    private final String path;
    private final int objectDepth;
    // We don't need atomicity, but we need to pass counter by reference to accumulate total value from sub-objects.
    private final AtomicInteger nestedFieldsCount;
    private final Set<String> eligibleCopyToDestinations;
    private final DynamicMapping parentDynamicMapping;

    Context(DataGeneratorSpecification specification, DynamicMapping parentDynamicMapping) {
        this(specification, "", 0, new AtomicInteger(0), new HashSet<>(), parentDynamicMapping);
    }

    private Context(
        DataGeneratorSpecification specification,
        String path,
        int objectDepth,
        AtomicInteger nestedFieldsCount,
        Set<String> eligibleCopyToDestinations,
        DynamicMapping parentDynamicMapping
    ) {
        this.specification = specification;
        this.childFieldGenerator = specification.dataSource().get(new DataSourceRequest.ChildFieldGenerator(specification));
        this.objectArrayGenerator = specification.dataSource().get(new DataSourceRequest.ObjectArrayGenerator());
        this.path = path;
        this.objectDepth = objectDepth;
        this.nestedFieldsCount = nestedFieldsCount;
        this.eligibleCopyToDestinations = eligibleCopyToDestinations;
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

    public Context subObject(String name, DynamicMapping dynamicMapping) {
        return new Context(
            specification,
            pathToField(name),
            objectDepth + 1,
            nestedFieldsCount,
            eligibleCopyToDestinations,
            dynamicMapping
        );
    }

    public Context nestedObject(String name, DynamicMapping dynamicMapping) {
        nestedFieldsCount.incrementAndGet();
        // copy_to can't be used across nested documents so all currently eligible fields are not eligible inside nested document.
        return new Context(specification, pathToField(name), objectDepth + 1, nestedFieldsCount, new HashSet<>(), dynamicMapping);
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
        if (objectDepth >= specification.maxObjectDepth()
            || nestedFieldsCount.get() >= specification.nestedFieldsLimit()
            || parentDynamicMapping == DynamicMapping.FORCED) {
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

    public Set<String> getEligibleCopyToDestinations() {
        return eligibleCopyToDestinations;
    }

    public void markFieldAsEligibleForCopyTo(String field) {
        eligibleCopyToDestinations.add(pathToField(field));
    }

    private String pathToField(String field) {
        return path.isEmpty() ? field : path + "." + field;
    }
}
