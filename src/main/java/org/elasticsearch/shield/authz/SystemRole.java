/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public class SystemRole extends Permission.Global {

    public static final String NAME = "__es_system_role";
    private static final Predicate<String> PREDICATE = Privilege.INTERNAL.predicate();

    @Override
    public boolean check(String action, TransportRequest request, MetaData metaData) {
        return PREDICATE.apply(action);
    }
}
