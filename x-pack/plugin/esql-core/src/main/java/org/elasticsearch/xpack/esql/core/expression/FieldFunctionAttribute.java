/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;

/**
 * A field attribute that has a function applied to it for value extraction.
 * This is used to replace a field attribute with a function that can extract
 * the value in a way that is more efficient than loading the raw field value
 * and applying the function to it.
 */
public class FieldFunctionAttribute extends FieldAttribute {

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "FieldFunctionAttribute",
        FieldFunctionAttribute::readFrom
    );

    private final Function function;

    private FieldFunctionAttribute(Source source, String parentName, String qualifier, String name, EsField field,
                                  Nullability nullability, NameId id, boolean synthetic, Function function) {
        super(source, parentName, qualifier, name, field, nullability, id, synthetic);
        this.function = function;
    }

    public static FieldFunctionAttribute fromFieldAttribute(
        FieldAttribute fieldAttribute,
        Function function
    ) {
        NameId nameId = new NameId();
        return new FieldFunctionAttribute(fieldAttribute.source(), fieldAttribute.parentName(), fieldAttribute.qualifier(),
            Attribute.rawTemporaryName(fieldAttribute.name(), "replaced", nameId.toString()),
            fieldAttribute.field(), fieldAttribute.nullable(), nameId, fieldAttribute.synthetic(), function);
    }

    public static FieldFunctionAttribute readFrom(StreamInput in) throws IOException {
        FieldAttribute fieldAttribute = FieldAttribute.readFrom(in);
        Function function = (Function) in.readNamedWriteable(Expression.class);
        return new FieldFunctionAttribute(
            fieldAttribute.source(),
            fieldAttribute.parentName(),
            fieldAttribute.qualifier(),
            fieldAttribute.name(),
            fieldAttribute.field(),
            fieldAttribute.nullable(),
            fieldAttribute.id(),
            fieldAttribute.synthetic(),
            function
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(function);
    }

    @Override
    public DataType dataType() {
        return function.dataType();
    }

    public Function getFunction() {
        return function;
    }

    protected String label() {
        return "t";
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}
