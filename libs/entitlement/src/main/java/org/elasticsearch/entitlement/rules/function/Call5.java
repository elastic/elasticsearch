/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules.function;

import java.io.Serializable;

public interface Call5<R, A, B, C, D, E> extends Serializable, VarargCallAdapter<R> {
    R call(A arg0, B arg1, C arg2, D arg3, E arg4) throws Exception;

    @SuppressWarnings("unchecked")
    @Override
    default VarargCall<R> asVarargCall() {
        return args -> call((A) args[0], (B) args[1], (C) args[2], (D) args[3], (E) args[4]);
    }
}
