/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import static org.elasticsearch.action.ActionListenerImplementations.safeOnFailure;

/**
 * A wrapper around an {@link ActionListener} which delegates failures safely to the inner listener's {@link ActionListener#onFailure}
 * method and which has a {@link #toString()} implementation which describes this class and the delegate.
 */
public abstract class DelegatingActionListener<Response, DelegateResponse> implements ActionListener<Response> {

    protected final ActionListener<DelegateResponse> delegate;

    protected DelegatingActionListener(ActionListener<DelegateResponse> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onFailure(Exception e) {
        safeOnFailure(delegate, e);
    }

    @Override
    public String toString() {
        return getClass().getName() + "/" + delegate;
    }
}
