/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;

import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.not;

public class GetBuiltinPrivilegesResponseTranslatorTests extends ESTestCase {

    public void testDefaultTranslatorHidesBoostPrivileges() {
        final List<String> clusterPrivileges = List.of(
            "all",
            "monitor",
            ClusterPrivilegeResolver.MONITOR_BOOST_RULES.name(),
            ClusterPrivilegeResolver.MONITOR_BOOST_PROFILES.name(),
            "monitor_ml"
        );
        final GetBuiltinPrivilegesResponse response = new GetBuiltinPrivilegesResponse(
            clusterPrivileges,
            List.of("read", "write"),
            List.of("monitor_enrich")
        );

        final GetBuiltinPrivilegesResponse translated = new GetBuiltinPrivilegesResponseTranslator.Default().translate(response);

        assertThat(translated.getClusterPrivileges(), arrayContaining("all", "monitor", "monitor_ml"));
        assertThat(translated.getClusterPrivileges(), not(hasItemInArray(ClusterPrivilegeResolver.MONITOR_BOOST_PROFILES.name())));
        assertThat(translated.getIndexPrivileges(), arrayContaining("read", "write"));
        assertThat(translated.getRemoteClusterPrivileges(), arrayContaining("monitor_enrich"));
    }
}
