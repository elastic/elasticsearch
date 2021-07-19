/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.action.ActionListener;

import static org.mockito.Matchers.any;

public abstract class ActionListenerUtils {

    @SuppressWarnings("unchecked")
    public static <T> ActionListener<T> anyActionListener() {
        return any(ActionListener.class);
    }
}
