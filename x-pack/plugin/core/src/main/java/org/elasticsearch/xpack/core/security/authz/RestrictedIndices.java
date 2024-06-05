/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.function.Predicate;

/**
 * Encapsulates security's model of which indices are "restricted"
 * @see RoleDescriptor.IndicesPrivileges#allowRestrictedIndices()
 * @see IndexNameExpressionResolver#getSystemNameAutomaton()
 */
public class RestrictedIndices {

    private final Automaton automaton;
    private final Predicate<String> predicate;

    public RestrictedIndices(IndexNameExpressionResolver resolver) {
        this(resolver.getSystemNameAutomaton());
    }

    public RestrictedIndices(Automaton automaton) {
        this.automaton = automaton;
        this.predicate = Automatons.predicate(automaton);
    }

    public boolean isRestricted(String indexOrAliasName) {
        return predicate.test(indexOrAliasName);
    }

    public Automaton getAutomaton() {
        return automaton;
    }
}
