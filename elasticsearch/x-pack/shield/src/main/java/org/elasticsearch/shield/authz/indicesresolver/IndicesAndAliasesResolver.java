/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.indicesresolver;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.transport.TransportRequest;

import java.util.Set;

/**
 *
 */
public interface IndicesAndAliasesResolver<Request extends TransportRequest> {

    Class<Request> requestType();

    Set<String> resolve(User user, String action, Request request, MetaData metaData);

}
