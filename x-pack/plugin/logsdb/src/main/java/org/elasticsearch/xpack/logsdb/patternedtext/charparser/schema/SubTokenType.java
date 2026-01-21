/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.TimestampComponentType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.Type;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.IntConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.IntConstraints;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringConstraints;

public class SubTokenType implements Type {
    public static final String ADHOC_PREFIX = "adhoc_";

    private final String name;
    private final String description;
    private final SubTokenBaseType baseType;
    private final String rawConstraint;
    private final IntConstraint intConstraint;
    private final StringConstraint stringConstraint;
    private final TimestampComponentType timestampComponentType;

    public SubTokenType(String name, SubTokenBaseType baseType, String constraint, String description) {
        this(name, baseType, constraint, description, TimestampComponentType.NA);
    }

    public SubTokenType(
        String name,
        SubTokenBaseType baseType,
        String constraint,
        String description,
        TimestampComponentType timestampComponentType
    ) {
        this.name = name;
        this.baseType = baseType;
        this.rawConstraint = constraint;
        this.description = description;
        this.timestampComponentType = timestampComponentType;

        Class<?> type = baseType.baseType();

        if (int.class.equals(type)) {
            this.intConstraint = IntConstraints.parseIntConstraint(constraint);
            this.stringConstraint = null;
        } else if (String.class.equals(type)) {
            this.intConstraint = null;
            this.stringConstraint = StringConstraints.parseStringConstraint(constraint);
        } else {
            throw new IllegalArgumentException("Unsupported base type: " + baseType.baseType());
        }
    }

    public String name() {
        return name;
    }

    public SubTokenBaseType getBaseType() {
        return baseType;
    }

    public String getDescriptor() {
        if (name.startsWith(SubTokenType.ADHOC_PREFIX)) {
            return "%" + baseType.symbol() + rawConstraint;
        }
        return name;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public EncodingType encodingType() {
        return baseType.encodingType();
    }

    public IntConstraint getIntConstraint() {
        return intConstraint;
    }

    public StringConstraint getStringConstraint() {
        return stringConstraint;
    }

    public TimestampComponentType getTimestampComponentType() {
        return timestampComponentType;
    }

    public char[] getValidCharacters() {
        if (intConstraint != null) {
            return baseType.allowedCharacters();
        }
        if (stringConstraint != null) {
            char[] validCharacters = stringConstraint.getValidCharacters();
            if (validCharacters == null) {
                // if no specific characters are defined, return the base type's allowed characters
                return baseType.allowedCharacters();
            }
            return validCharacters;
        }
        // no constraints defined, return the base type's allowed characters
        return baseType.allowedCharacters();
    }
}
