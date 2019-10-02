/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;

import java.util.function.Supplier;

public interface MlDataRemover {
    void remove(ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier);
}
