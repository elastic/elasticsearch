/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;

public interface DataLifecyclePrivilegesCheck {
    void checkCanConfigure(String[] dataStreamPatterns, ActionListener<Void> listener);

    class Noop implements DataLifecyclePrivilegesCheck {
        @Inject
        public Noop() {}

        @Override
        public void checkCanConfigure(String[] dataStreamPatterns, ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }
}
