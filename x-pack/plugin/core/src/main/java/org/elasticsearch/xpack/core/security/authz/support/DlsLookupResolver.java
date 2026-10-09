/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Subject;

import java.util.Map;

/**
 * Resolves the value of a {@link DlsLookup} declared by a templated DLS role query.
 * <p>
 * Resolvers are registered by type name through
 * {@link org.elasticsearch.xpack.core.security.SecurityExtension#getDlsLookupResolvers}. When an index action is authorized on
 * the coordinating node and the resulting access control carries DLS queries that declare lookups, every lookup that has not
 * already been resolved for the request is handed to the resolver registered for its type. The values are then carried to the
 * shard level on the thread context, so a lookup is resolved at most once per request even though shard-level actions are
 * authorized again on every data node. This also means every shard observes the same snapshot of the looked-up data.
 * <p>
 * A resolver receives the {@link DlsLookup#params() params} written into the role query, which are known when the privilege is
 * synthesized, and the effective {@link Subject} of the request, which is only known at authorization time. Under run-as the
 * subject is the impersonated user, matching the user whose details the template sees under {@code _user}.
 * <p>
 * The resolved value must be a JSON-compatible object (a {@link String}, {@link Number}, {@link Boolean}, {@link Iterable} or
 * {@link Map} of the same) and must not be {@code null}: it is rendered into the template under {@code _lookup.<name>} and
 * serialized when carried between nodes. Implementations should produce deterministic output for the same inputs, for
 * instance by sorting collections, so that the rendered query and the shard request cache key are stable.
 * <p>
 * A failure reported to the listener fails the authorized request. This is deliberate: applying a DLS template against partial
 * or missing data could widen the set of visible documents.
 */
public interface DlsLookupResolver {

    /**
     * Asynchronously resolves a lookup value.
     *
     * @param params           the params declared by the role query, never {@code null} but possibly empty
     * @param effectiveSubject the subject the request is authorized as
     * @param listener         receives the non-null value, or the failure that will abort the request
     */
    void resolve(Map<String, Object> params, Subject effectiveSubject, ActionListener<Object> listener);
}
