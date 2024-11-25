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
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.SEMANTIC_TEXT;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamInput.readCachedStringWithVersionCheck;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

public class SemanticTextEsField extends EsField {
    private final Set<String> inferenceIds;

    public SemanticTextEsField(
        String name,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        Set<String> inferenceIds
    ) {
        super(name, SEMANTIC_TEXT, properties, aggregatable, isAlias);
        this.inferenceIds = inferenceIds;
    }

    public SemanticTextEsField(StreamInput in) throws IOException {
        this(
            readCachedStringWithVersionCheck(in),
            in.readImmutableMap(EsField::readFrom),
            in.readBoolean(),
            in.readBoolean(),
            in.readCollectionAsSet(StreamInput::readString)
        );
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        writeCachedStringWithVersionCheck(out, getName());
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
        out.writeBoolean(isAggregatable());
        out.writeBoolean(isAlias());
        out.writeCollection(inferenceIds, StreamOutput::writeString);
    }

    public String getWriteableName() {
        return "SemanticTextEsField";
    }

    public Set<String> inferenceIds() {
        return inferenceIds;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        return Objects.equals(inferenceIds, ((SemanticTextEsField) obj).inferenceIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceIds);
    }

}
