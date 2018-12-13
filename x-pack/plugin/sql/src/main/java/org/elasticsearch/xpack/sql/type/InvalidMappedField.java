/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.sql.analysis.index.MappingException;

import java.util.Objects;

import static java.util.Collections.emptyMap;

/**
 * Representation of field mapped differently across indices.
 * Used during mapping discovery only.
 */
public class InvalidMappedField extends EsField {

    private final String errorMessage;

    public InvalidMappedField(String name, String errorMessage) {
        super(name, DataType.UNSUPPORTED, emptyMap(), false);
        this.errorMessage = errorMessage;
    }

    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), errorMessage);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InvalidMappedField other = (InvalidMappedField) obj;
            return Objects.equals(errorMessage, other.errorMessage);
        }
        
        return false;
    }

    @Override
    public EsField getExactField() {
        throw new MappingException("Field [" + getName() + "] is invalid, cannot access it");

    }

    @Override
    public boolean isExact() {
        return false;
    }
}