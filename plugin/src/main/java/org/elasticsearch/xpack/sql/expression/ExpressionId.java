/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Objects;

public class ExpressionId {

    private final String id;
    private final String jvmId;

    ExpressionId(String id, String jvmId) {
        this.id = id;
        this.jvmId = jvmId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jvmId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExpressionId other = (ExpressionId) obj;
        return Objects.equals(id, other.id) 
                && Objects.equals(jvmId, other.jvmId);
    }

    @Override
    public String toString() {
        return id;
        //#+ jvmId;
    }
}
