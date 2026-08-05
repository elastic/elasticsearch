/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used as a marker for fields that may be unmapped, where an unmapped field is a field which exists in the _source but is not
 * mapped in the index. Note that this field may be mapped for some indices, but is unmapped in at least one of them.
 * For indices where the field is unmapped, we will try to load them directly from _source.
 */
public class PotentiallyUnmappedKeywordEsField extends KeywordEsField {
    private static final TransportVersion ESQL_UNMAPPED_KEYWORD_LEAF_NAME = TransportVersion.fromName("esql_unmapped_keyword_leaf_name");

    public PotentiallyUnmappedKeywordEsField(String name) {
        // Use a mutable map: IndexResolver may add child fields into the properties when the keyword field
        // has multi-fields (e.g. "my_field.analyzed") that are also partially unmapped.
        this(name, new HashMap<>());
    }

    // Visible for testing
    public PotentiallyUnmappedKeywordEsField(String name, Map<String, EsField> properties) {
        super(name, properties, true, Short.MAX_VALUE, false, false, TimeSeriesFieldType.UNKNOWN);
    }

    public PotentiallyUnmappedKeywordEsField(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Serializes this field, but writes {@code fullName} (the full dotted path) as the field name for nodes predating
     * {@code esql_unmapped_keyword_leaf_name}: they match unmapped fields by the EsField name in
     * {@code DefaultShardContextForUnmappedField}, whereas this field now holds only the leaf name. The caller supplies
     * {@code fullName} because the parent path lives on the enclosing {@code FieldAttribute}, not on the field.
     */
    public void writeTo(StreamOutput out, String fullName) throws IOException {
        if (out.getTransportVersion().supports(ESQL_UNMAPPED_KEYWORD_LEAF_NAME)) {
            writeTo(out);
        } else {
            new PotentiallyUnmappedKeywordEsField(fullName, getProperties()).writeTo(out);
        }
    }

    public String getWriteableName(TransportVersion transportVersion) {
        return "PotentiallyUnmappedKeywordEsField";
    }

    @Override
    public String getNodeStringName() {
        return "PotentiallyUnmappedKeywordEsField";
    }
}
