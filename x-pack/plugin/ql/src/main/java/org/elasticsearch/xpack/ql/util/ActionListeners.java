/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;

import java.util.function.Consumer;

public class ActionListeners {

    private ActionListeners() {}

    /**
     * Combination of {@link ActionListener#wrap(CheckedConsumer, Consumer)} and {@link ActionListener#map(ActionListener, CheckedFunction)}
     */
    public static <T, Response> ActionListener<Response> map(ActionListener<T> delegate, CheckedFunction<Response, T, Exception> fn) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                T mapped;
                try {
                    mapped = fn.apply(response);
                } catch (Exception e) {
                    onFailure(e);
                    return;
                }
                try {
                    delegate.onResponse(mapped);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }
}
