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

import java.util.Optional;

class Context {
    private final DataGeneratorSpecification specification;

    private final DataSourceResponse.ChildFieldGenerator childFieldGenerator;
    private final DataSourceResponse.FieldTypeGenerator fieldTypeGenerator;
    private final DataSourceResponse.ObjectArrayGenerator objectArrayGenerator;
    private final int objectDepth;
    private final int nestedFieldsCount;

    Context(DataGeneratorSpecification specification) {
        this(specification, 0, 0);
    }

    private Context(DataGeneratorSpecification specification, int objectDepth, int nestedFieldsCount) {
        this.specification = specification;
        this.childFieldGenerator = specification.dataSource().get(new DataSourceRequest.ChildFieldGenerator(specification));
        this.fieldTypeGenerator = specification.dataSource().get(new DataSourceRequest.FieldTypeGenerator());
        this.objectArrayGenerator = specification.dataSource().get(new DataSourceRequest.ObjectArrayGenerator());
        this.objectDepth = objectDepth;
        this.nestedFieldsCount = nestedFieldsCount;
    }

    public DataGeneratorSpecification specification() {
        return specification;
    }

    public DataSourceResponse.ChildFieldGenerator childFieldGenerator() {
        return childFieldGenerator;
    }

    public DataSourceResponse.FieldTypeGenerator fieldTypeGenerator() {
        return fieldTypeGenerator;
    }

    public Context subObject() {
        return new Context(specification, objectDepth + 1, nestedFieldsCount);
    }

    public Context nestedObject() {
        return new Context(specification, objectDepth + 1, nestedFieldsCount + 1);
    }

    public boolean shouldAddObjectField() {
        return childFieldGenerator.generateRegularSubObject() && objectDepth < specification.maxObjectDepth();
    }

    public boolean shouldAddNestedField() {
        return childFieldGenerator.generateNestedSubObject()
            && objectDepth < specification.maxObjectDepth()
            && nestedFieldsCount < specification.nestedFieldsLimit();
    }

    public Optional<Integer> generateObjectArray() {
        if (objectDepth == 0) {
            return Optional.empty();
        }

        return objectArrayGenerator.lengthGenerator().get();
    }
}
