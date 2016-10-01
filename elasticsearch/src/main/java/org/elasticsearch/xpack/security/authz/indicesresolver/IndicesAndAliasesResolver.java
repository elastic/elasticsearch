/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.indicesresolver;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.transport.TransportRequest;

import java.util.Set;

/**
 *
 */
public interface IndicesAndAliasesResolver<Request extends TransportRequest> {

    Class<Request> requestType();

    Set<String> resolve(User user, String action, Request request, MetaData metaData);

}
