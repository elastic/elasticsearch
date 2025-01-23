/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Strings;

import java.io.IOException;

// FIXME(gal, do-not-merge!) document
public class PartiallyUnmappedField extends EsField {
    private final EsField mappedField;
    private final boolean insisted;

    private PartiallyUnmappedField(EsField mappedField, DataType dataType, boolean insisted) {
        super(
            mappedField.getName(),
            dataType,
            mappedField.getProperties(),
            dataType != DataType.UNSUPPORTED && mappedField.isAggregatable()
        );
        this.mappedField = mappedField;
        this.insisted = insisted;
    }

    public PartiallyUnmappedField(EsField mappedField) {
        this(mappedField, switch (mappedField.getDataType()) {
            case KEYWORD -> DataType.KEYWORD;
            default -> DataType.UNSUPPORTED;
        }, false /* insisted */);
    }

    public EsField mappedField() {
        return mappedField;
    }

    public PartiallyUnmappedField markInsisted() {
        return new PartiallyUnmappedField(mappedField, DataType.KEYWORD, true /* insisted */);
    }

    public boolean isInsisted() {
        return insisted;
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        mappedField.writeTo(out);
    }

    PartiallyUnmappedField(StreamInput in) throws IOException {
        this(EsField.readFrom(in));
    }

    @Override
    public String getWriteableName() {
        return "PartiallyUnmappedField";
    }

    @Override
    public String toString() {
        return Strings.format("PartiallyUnmappedField[%s]", mappedField);
    }
}
