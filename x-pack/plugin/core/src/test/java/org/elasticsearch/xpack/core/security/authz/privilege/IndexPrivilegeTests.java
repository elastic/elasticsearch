/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.findPrivilegesThatGrant;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class IndexPrivilegeTests extends ESTestCase {

    /**
     * The {@link IndexPrivilege#values()} map is sorted so that privilege names that offer the _least_ access come before those that
     * offer _more_ access. There is no guarantee of ordering between privileges that offer non-overlapping privileges.
     */
    public void testOrderingOfPrivilegeNames() throws Exception {
        final Set<String> names = IndexPrivilege.values().keySet();
        final int all = Iterables.indexOf(names, "all"::equals);
        final int manage = Iterables.indexOf(names, "manage"::equals);
        final int monitor = Iterables.indexOf(names, "monitor"::equals);
        final int read = Iterables.indexOf(names, "read"::equals);
        final int write = Iterables.indexOf(names, "write"::equals);
        final int index = Iterables.indexOf(names, "index"::equals);
        final int create_doc = Iterables.indexOf(names, "create_doc"::equals);
        final int delete = Iterables.indexOf(names, "delete"::equals);

        assertThat(read, lessThan(all));
        assertThat(manage, lessThan(all));
        assertThat(monitor, lessThan(manage));
        assertThat(write, lessThan(all));
        assertThat(index, lessThan(write));
        assertThat(create_doc, lessThan(index));
        assertThat(delete, lessThan(write));
    }

    public void testFindPrivilegesThatGrant() {
        assertThat(findPrivilegesThatGrant(SearchAction.NAME), equalTo(List.of("read", "all")));
        assertThat(findPrivilegesThatGrant(IndexAction.NAME), equalTo(List.of("create_doc", "create", "index", "write", "all")));
        assertThat(findPrivilegesThatGrant(UpdateAction.NAME), equalTo(List.of("index", "write", "all")));
        assertThat(findPrivilegesThatGrant(DeleteAction.NAME), equalTo(List.of("delete", "write", "all")));
        assertThat(findPrivilegesThatGrant(IndicesStatsAction.NAME), equalTo(List.of("monitor", "manage", "all")));
        assertThat(findPrivilegesThatGrant(RefreshAction.NAME), equalTo(List.of("maintenance", "manage", "all")));
        assertThat(findPrivilegesThatGrant(ShrinkAction.NAME), equalTo(List.of("manage", "all")));
    }

}
