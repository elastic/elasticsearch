/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;

import java.util.Collections;
import java.util.Set;

/**
 * A ConditionalClusterPrivilege is a {@link ClusterPrivilege} that determines which cluster actions
 * may be executed based on the {@link ConditionalPrivilege#getRequestPredicate()}.
 */
public abstract class ConditionalClusterPrivilege extends ClusterPrivilege implements ConditionalPrivilege {

    ConditionalClusterPrivilege(String name, String... patterns) {
        super(name, patterns);
    }

    ConditionalClusterPrivilege(String name, Automaton automaton) {
        super(Collections.singleton(name), automaton);
    }

    ConditionalClusterPrivilege(Set<String> name, Automaton automaton) {
        super(name, automaton);
    }

}
