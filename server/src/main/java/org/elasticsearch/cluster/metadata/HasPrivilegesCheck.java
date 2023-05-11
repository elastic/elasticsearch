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

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public interface HasPrivilegesCheck {

    void getPrivilegesCheck(ActionListener<Function<PrivilegesToCheck, Exception>> listener);

    record IndexPrivileges(List<String> indices, List<String> privileges) {
        IndexPrivileges(List<String> indices, String privilege) {
            this(indices, List.of(privilege));
        }
    }

    record PrivilegesToCheck(List<IndexPrivileges> indexPrivileges, List<String> clusterPrivileges) {

        PrivilegesToCheck(IndexPrivileges indexPrivileges) {
            this(List.of(indexPrivileges), Collections.emptyList());
        }

    }

    class Noop implements HasPrivilegesCheck {
        @Inject
        public Noop() {}

        @Override
        public void getPrivilegesCheck(ActionListener<Function<PrivilegesToCheck, Exception>> listener) {
            listener.onResponse(ignored -> null);
        }
    }
}
