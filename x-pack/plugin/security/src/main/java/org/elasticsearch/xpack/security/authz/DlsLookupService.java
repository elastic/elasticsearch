/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.support.DlsLookup;
import org.elasticsearch.xpack.core.security.authz.support.DlsLookupResolver;
import org.elasticsearch.xpack.core.security.authz.support.ResolvedDlsLookups;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Resolves the {@link DlsLookup}s declared by a request's DLS queries using the {@link DlsLookupResolver}s registered by security
 * extensions, keyed by lookup type.
 * <p>
 * Resolution is fail-closed: an unregistered type, a resolver failure, or a {@code null} value fails the whole request rather than
 * letting a template render against missing data. Lookups that are already present in the request's {@link ResolvedDlsLookups}
 * are not resolved again, which is what bounds resolution to once per request even though shard-level actions are authorized on
 * every data node they reach.
 */
public class DlsLookupService {

    private static final Logger logger = LogManager.getLogger(DlsLookupService.class);

    private final Map<String, DlsLookupResolver> resolvers;

    public DlsLookupService(Map<String, DlsLookupResolver> resolvers) {
        this.resolvers = Map.copyOf(Objects.requireNonNull(resolvers));
    }

    public boolean hasResolver(String type) {
        return resolvers.containsKey(type);
    }

    /**
     * Resolves every lookup not already present in {@code alreadyResolved}, in parallel, and completes the listener with the
     * union. Lookups with equal {@link DlsLookup#key() keys} are resolved once. If nothing needs resolving, the listener is
     * completed with {@code alreadyResolved} itself, so callers can detect that no new values were produced by identity.
     *
     * @param lookups          the lookups declared by the request's access control
     * @param alreadyResolved  the values already carried by the request
     * @param effectiveSubject the subject the request is authorized as
     * @param listener         receives the resolved values, or the first failure
     */
    public void resolve(
        Collection<DlsLookup> lookups,
        ResolvedDlsLookups alreadyResolved,
        Subject effectiveSubject,
        ActionListener<ResolvedDlsLookups> listener
    ) {
        final Map<String, DlsLookup> pending = new LinkedHashMap<>();
        for (DlsLookup lookup : lookups) {
            if (alreadyResolved.contains(lookup) == false) {
                pending.putIfAbsent(lookup.key(), lookup);
            }
        }
        if (pending.isEmpty()) {
            listener.onResponse(alreadyResolved);
            return;
        }
        // Check every type up front so that a misconfigured lookup fails before any resolver is invoked.
        for (DlsLookup lookup : pending.values()) {
            if (resolvers.containsKey(lookup.type()) == false) {
                listener.onFailure(
                    new IllegalStateException(
                        "no DLS lookup resolver is registered for type [" + lookup.type() + "] declared by lookup [" + lookup.name() + "]"
                    )
                );
                return;
            }
        }
        logger.trace("resolving [{}] DLS lookup(s) for subject [{}]", pending.size(), effectiveSubject);
        final GroupedActionListener<Tuple<String, Object>> grouped = new GroupedActionListener<>(pending.size(), listener.map(results -> {
            final Map<String, Object> valuesByKey = new HashMap<>();
            for (Tuple<String, Object> result : results) {
                valuesByKey.put(result.v1(), result.v2());
            }
            return alreadyResolved.merge(valuesByKey);
        }));
        for (DlsLookup lookup : pending.values()) {
            final DlsLookupResolver resolver = resolvers.get(lookup.type());
            final String key = lookup.key();
            ActionListener.run(grouped.map(value -> {
                if (value == null) {
                    throw new IllegalStateException(
                        "DLS lookup resolver for type [" + lookup.type() + "] returned null for lookup [" + lookup.name() + "]"
                    );
                }
                return new Tuple<>(key, value);
            }), l -> resolver.resolve(lookup.params(), effectiveSubject, l));
        }
    }
}
