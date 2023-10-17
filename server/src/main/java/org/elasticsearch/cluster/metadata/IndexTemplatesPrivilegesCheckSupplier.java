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

import java.util.List;
import java.util.function.Consumer;

public interface IndexTemplatesPrivilegesCheckSupplier {

    void getPrivilegesCheckForIndexPatterns(ActionListener<Consumer<List<String>>> listener);

    class Noop implements IndexTemplatesPrivilegesCheckSupplier {
        @Inject
        public Noop() {}

        @Override
        public void getPrivilegesCheckForIndexPatterns(ActionListener<Consumer<List<String>>> listener) {
            listener.onResponse(ignored -> {});
        }
    }
}
