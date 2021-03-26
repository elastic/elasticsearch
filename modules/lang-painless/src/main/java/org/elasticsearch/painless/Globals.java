/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Program-wide globals (initializers, synthetic methods, etc)
 */
public class Globals {
    private final Map<String,Constant> constantInitializers = new HashMap<>();
    private final BitSet statements;

    /** Create a new Globals from the set of statement boundaries */
    public Globals(BitSet statements) {
        this.statements = statements;
    }

    /** Adds a new constant initializer to be written */
    public void addConstantInitializer(Constant constant) {
        if (constantInitializers.put(constant.name, constant) != null) {
            throw new IllegalStateException("constant initializer: " + constant.name + " already exists");
        }
    }

    /** Returns the current initializers */
    public Map<String,Constant> getConstantInitializers() {
        return constantInitializers;
    }

    /** Returns the set of statement boundaries */
    public BitSet getStatements() {
        return statements;
    }
}
