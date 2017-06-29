/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import java.util.function.Function;

import org.elasticsearch.action.ActionListener;

public abstract class ActionUtils {

    public static <Input, Output> ActionListener<Input> chain(ActionListener<Output> outputListener, Function<Input, Output> action) {
        return ActionListener.wrap(i -> outputListener.onResponse(action.apply(i)), outputListener::onFailure);
    }
}
