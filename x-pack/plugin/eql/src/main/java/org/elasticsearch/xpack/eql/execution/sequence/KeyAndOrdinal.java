/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.Objects;

public class KeyAndOrdinal {
    final SequenceKey key;
    final Ordinal ordinal;

    public KeyAndOrdinal(SequenceKey key, Ordinal ordinal) {
        this.key = key;
        this.ordinal = ordinal;
    }

    public SequenceKey key() {
        return key;
    }

    public Ordinal ordinal() {
        return ordinal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, ordinal);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        KeyAndOrdinal other = (KeyAndOrdinal) obj;
        return Objects.equals(key, other.key)
                && Objects.equals(ordinal, other.ordinal);
    }

    @Override
    public String toString() {
        return key.toString() + ordinal.toString();
    }
}
