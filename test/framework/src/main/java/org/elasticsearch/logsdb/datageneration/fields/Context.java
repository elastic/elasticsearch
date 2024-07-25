/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;

class Context {
    private final DataGeneratorSpecification specification;
    private final int objectDepth;
    private final int nestedFieldsCount;

    Context(DataGeneratorSpecification specification) {
        this(specification, 0, 0);
    }

    private Context(DataGeneratorSpecification specification, int objectDepth, int nestedFieldsCount) {
        this.specification = specification;
        this.objectDepth = objectDepth;
        this.nestedFieldsCount = nestedFieldsCount;
    }

    public DataGeneratorSpecification specification() {
        return specification;
    }

    public Context subObject() {
        return new Context(specification, objectDepth + 1, nestedFieldsCount);
    }

    public Context nestedObject() {
        return new Context(specification, objectDepth + 1, nestedFieldsCount + 1);
    }

    public boolean shouldAddObjectField() {
        return specification.arbitrary().generateSubObject() && objectDepth < specification.maxObjectDepth();
    }

    public boolean shouldAddNestedField() {
        return specification.arbitrary().generateNestedObject()
            && objectDepth < specification.maxObjectDepth()
            && nestedFieldsCount < specification.nestedFieldsLimit();
    }
}
