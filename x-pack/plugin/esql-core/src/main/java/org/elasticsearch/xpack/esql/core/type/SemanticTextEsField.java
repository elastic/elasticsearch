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
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.SEMANTIC_TEXT;

public class SemanticTextEsField extends EsField {
    private final List<String> inferenceIds;

    public SemanticTextEsField(
        String name,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        List<String> infereceIds
    ) {
        super(name, SEMANTIC_TEXT, properties, aggregatable, isAlias);
        this.inferenceIds = infereceIds;
    }

    public SemanticTextEsField(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readImmutableMap(EsField::readFrom),
            in.readBoolean(),
            in.readBoolean(),
            in.readCollectionAsList(StreamInput::readString)
        );
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        out.writeString(getName());
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
        out.writeBoolean(isAggregatable());
        out.writeBoolean(isAlias());
        out.writeCollection(inferenceIds, StreamOutput::writeString);
    }

    public String getWriteableName() {
        return "SemanticTextEsField";
    }

    public List<String> inferenceIds() {
        return inferenceIds;
    }

}
