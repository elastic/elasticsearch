/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES;

public class AutomatonPatternsTests extends ESTestCase {

    /**
     * RCS 2.0 allows a single API key to define "replication" and "search" blocks. If both are defined, this results in an API key with 2
     * sets of indices permissions. Due to the way API keys (and roles) work across the multiple index permission, the set of index
     * patterns allowed are effectively the most generous of the sets of index patterns since the index patterns are OR'ed together. For
     * example, `foo` OR `*` results in access to `*`. So, if you have "search" access defined as `foo`, but replication access defined
     * as `*`, the API key effectively allows access to index pattern `*`. This means that the access for API keys that define both
     * "search" and "replication", the action names used are the primary means by which we can constrain CCS to the set of "search" indices
     * as well as how we constrain CCR to the set "replication" indices. For example, if "replication" ever allowed access to
     * `indices:data/read/get` for `*` , then the "replication" permissions would effectively enable users of CCS to get documents,
     * even if "search" is never defined in the RCS 2.0 API key. This obviously is not desirable and in practice when both "search" and
     * "replication" are defined the isolation between CCS and CCR is only achieved because the action names for the workflows do not
     * overlap. This test helps to ensure that the actions names used for RCS 2.0 do not bleed over between search and replication.
     */
    public void testRemoteClusterPrivsDoNotOverlap() {

        // check that the action patterns for remote CCS are not allowed by remote CCR privileges
        Arrays.stream(CCS_INDICES_PRIVILEGE_NAMES).forEach(ccsPrivilege -> {
            Automaton ccsAutomaton = IndexPrivilege.get(ccsPrivilege).getAutomaton();
            Automatons.getPatterns(ccsAutomaton).forEach(ccsPattern -> {
                // emulate an action name that could be allowed by a CCS privilege
                String actionName = ccsPattern.replaceAll("\\*", randomAlphaOfLengthBetween(1, 8));
                Arrays.stream(CCR_INDICES_PRIVILEGE_NAMES).forEach(ccrPrivileges -> {
                    String errorMessage = String.format(
                        Locale.ROOT,
                        "CCR privilege \"%s\" allows CCS action \"%s\". This could result in an "
                            + "accidental bleeding of permission between RCS 2.0's search and replication index permissions",
                        ccrPrivileges,
                        ccsPattern
                    );
                    assertFalse(errorMessage, IndexPrivilege.get(ccrPrivileges).predicate().test(actionName));
                });
            });
        });

        // check that the action patterns for remote CCR are not allowed by remote CCS privileges
        Arrays.stream(CCR_INDICES_PRIVILEGE_NAMES).forEach(ccrPrivilege -> {
            Automaton ccrAutomaton = IndexPrivilege.get(ccrPrivilege).getAutomaton();
            Automatons.getPatterns(ccrAutomaton).forEach(ccrPattern -> {
                // emulate an action name that could be allowed by a CCR privilege
                String actionName = ccrPattern.replaceAll("\\*", randomAlphaOfLengthBetween(1, 8));
                Arrays.stream(CCS_INDICES_PRIVILEGE_NAMES).forEach(ccsPrivileges -> {
                    if ("indices:data/read/xpack/ccr/shard_changes*".equals(ccrPattern)) {
                        // do nothing, this action is only applicable to CCR workflows and is a moot point if CCS technically has
                        // access to the index pattern for this action granted by CCR
                    } else {
                        String errorMessage = String.format(
                            Locale.ROOT,
                            "CCS privilege \"%s\" allows CCR action \"%s\". This could result in an accidental bleeding of "
                                + "permission between RCS 2.0's search and replication index permissions",
                            ccsPrivileges,
                            ccrPattern
                        );
                        assertFalse(errorMessage, IndexPrivilege.get(ccsPrivileges).predicate().test(actionName));
                    }
                });
            });
        });
    }
}
