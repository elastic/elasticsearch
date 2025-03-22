/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * This class is used as a marker for fields that may be unmapped, where an unmapped field is a field which exists in the _source but is not
 * mapped in the index. Note that this field may be mapped for some indices, but is unmapped in at least one of them.
 * For indices where the field is unmapped, we will try to load them directly from _source.
 */
public class PotentiallyUnmappedKeywordEsField extends KeywordEsField {
    public PotentiallyUnmappedKeywordEsField(String name) {
        super(name);
    }

    public PotentiallyUnmappedKeywordEsField(StreamInput in) throws IOException {
        super(in);
    }

    public String getWriteableName() {
        return "PotentiallyUnmappedKeywordEsField";
    }
}
