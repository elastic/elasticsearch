/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;

public interface DataLifecycleAuthorizationCheck {
    void check(String[] dataStreamPatterns, ActionListener<AcknowledgedResponse> listener);

    class Noop implements DataLifecycleAuthorizationCheck {
        @Inject
        public Noop() {}

        @Override
        public void check(String[] dataStreamPatterns, ActionListener<AcknowledgedResponse> listener) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        }
    }
}
