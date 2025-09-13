/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.esql.core.util.PlanStreamInput.readCachedStringWithVersionCheck;

/**
 * NOCOMMIT
 */
public interface DataType extends Writeable {
    /**
     * The type of this attribute. The result is an enum so you can {@code switch}
     * or {@code ==} on it.
     */
    AtomType atom();

    /**
     * The type of one of the fields inside this type, or {@code null} if this
     * field doesn't exist. Some types, like those from {@link #atom(AtomType)},
     * don't have any fields inside them and will return {@code null} for
     * all strings. They are <strong>just</strong> {@link #atom() atomic}.
     */
    AtomType field(String name);

    /**
     * Replace {@code text} fields with {@code keyword} fields.
     */
    DataType noText();

    /**
     * @return the estimated size, in bytes, of this data type.  If there's no reasonable way to estimate the size,
     *         the optional will be empty.
     */
    Optional<Integer> estimatedSize();

    /**
     * The name we give to types on the response.
     */
    String outputType();

    /**
     * Atomic type without any sub-fields.
     * @deprecated NOCOMMIT replace this with DataType.type
     */
    @Deprecated
    static DataType atom(AtomType type) {
        return type.type();
    }

    /**
     * Composite type made of many columns.
     */
    static DataType object(Map<String, AtomType> fields) {
        return new ObjectType(fields);
    }

    static DataType readFrom(StreamInput in) throws IOException {
        String type = readCachedStringWithVersionCheck(in);
        if ("o".equals(type)) {
            return new ObjectType(in);
        }
        return AtomType.Type.readFrom(type);
    }
}
