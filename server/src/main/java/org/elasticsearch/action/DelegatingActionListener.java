/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import static org.elasticsearch.action.ActionListenerImplementations.safeOnFailure;

/**
 * A wrapper around an {@link ActionListener} {@code L} that by default delegates failures to {@code L}'s {@link ActionListener#onFailure}
 * method. The wrapper also provides a {@link #toString()} implementation that describes this class and the delegate.
 * <p>
 * This is a useful base class for creating ActionListener wrappers that override the {@link #onResponse} handling, with access to
 * {@code L}, while retaining all of {@code L}'s other handling. It can also be useful to override other methods to do new work with access
 * to {@code L}.
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
