/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A listener that ensures that only one of onResponse or onFailure is called. And the method
 * the is called is only called once. Subclasses should implement notification logic with
 * innerOnResponse and innerOnFailure.
 */
public abstract class NotifyOnceListener<Response> implements ActionListener<Response> {

    private final AtomicBoolean hasBeenCalled = new AtomicBoolean(false);

    protected abstract void innerOnResponse(Response response);

    protected abstract void innerOnFailure(Exception e);

    @Override
    public final void onResponse(Response response) {
        if (hasBeenCalled.compareAndSet(false, true)) {
            innerOnResponse(response);
        }
    }

    @Override
    public final void onFailure(Exception e) {
        if (hasBeenCalled.compareAndSet(false, true)) {
            innerOnFailure(e);
        }
    }
}
