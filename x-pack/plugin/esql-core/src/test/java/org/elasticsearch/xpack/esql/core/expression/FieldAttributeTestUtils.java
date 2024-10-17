/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

public class FieldAttributeTestUtils {
    public static final FieldAttribute newFieldAttributeWithType(
        Source source,
        FieldAttribute parent,
        String name,
        DataType type,
        EsField field,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new FieldAttribute(source, parent, name, type, field, nullability, id, synthetic);
    }
}
