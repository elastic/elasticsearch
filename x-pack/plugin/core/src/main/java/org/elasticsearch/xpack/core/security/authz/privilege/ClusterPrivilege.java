/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;

import java.util.Collections;
import java.util.Set;

public final class ClusterPrivilege extends Privilege {

    ClusterPrivilege(String name, String... patterns) {
        super(name, patterns);
    }

    ClusterPrivilege(String name, Automaton automaton) {
        super(Collections.singleton(name), automaton);
    }

    ClusterPrivilege(Set<String> name, Automaton automaton) {
        super(name, automaton);
    }
}
