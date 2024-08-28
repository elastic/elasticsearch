/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Information about a field in an ES index with the {@code date} type
 */
public class DateEsField extends EsField {

    public static DateEsField dateEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        return new DateEsField(name, DataType.DATETIME, properties, hasDocValues);
    }

    private DateEsField(String name, DataType dataType, Map<String, EsField> properties, boolean hasDocValues) {
        super(name, dataType, properties, hasDocValues);
    }

    protected DateEsField(StreamInput in) throws IOException {
        this(in.readString(), DataType.DATETIME, in.readImmutableMap(EsField::readFrom), in.readBoolean());
    }

    @Override
    protected void writeContent(StreamOutput out) throws IOException {
        out.writeString(getName());
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
        out.writeBoolean(isAggregatable());
    }

    public String getWriteableName() {
        return "DateEsField";
    }

}
