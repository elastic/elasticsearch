/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class PrivilegeTests extends ElasticsearchTestCase {

    @Test
    public void testName() throws Exception {
        Privilege.Name name12 = new Privilege.Name("name1", "name2");
        Privilege.Name name34 = new Privilege.Name("name3", "name4");
        Privilege.Name name1234 = randomBoolean() ? name12.add(name34) : name34.add(name12);
        assertThat(name1234, equalTo(new Privilege.Name("name1", "name2", "name3", "name4")));

        Privilege.Name name1 = name12.remove(new Privilege.Name("name2"));
        assertThat(name1, equalTo(new Privilege.Name("name1")));

        Privilege.Name name = name1.remove(new Privilege.Name("name1"));
        assertThat(name, is(Privilege.Name.NONE));

        Privilege.Name none = new Privilege.Name("name1", "name2", "none").remove(name12);
        assertThat(none, is(Privilege.Name.NONE));
    }


    @Test
    public void testCluster() throws Exception {

        Privilege.Name name = new Privilege.Name("monitor");
        Privilege.Cluster cluster = Privilege.Cluster.get(name);
        assertThat(cluster, is(Privilege.Cluster.MONITOR));

        // since "all" implies "monitor", this should collapse to All
        name = new Privilege.Name("monitor", "all");
        cluster = Privilege.Cluster.get(name);
        assertThat(cluster, is(Privilege.Cluster.ALL));

        name = new Privilege.Name("monitor", "none");
        cluster = Privilege.Cluster.get(name);
        assertThat(cluster, is(Privilege.Cluster.MONITOR));

        Privilege.Name name2 = new Privilege.Name("none", "monitor");
        Privilege.Cluster cluster2 = Privilege.Cluster.get(name2);
        assertThat(cluster, is(cluster2));
    }

    @Test
    public void testIndex_Collapse() throws Exception {
        Privilege.Index[] values = Privilege.Index.values();
        Privilege.Index first = values[randomIntBetween(0, values.length-1)];
        Privilege.Index second = values[randomIntBetween(0, values.length-1)];

        Privilege.Name name = new Privilege.Name(first.name().toString(), second.name().toString());
        Privilege.Index index = Privilege.Index.get(name);

        if (first.implies(second)) {
            assertThat(index, is(first));
        }

        if (second.implies(first)) {
            assertThat(index, is(second));
        }
    }

    @Test
    public void testIndex_Implies() throws Exception {
        Privilege.Index[] values = Privilege.Index.values();
        Privilege.Index first = values[randomIntBetween(0, values.length-1)];
        Privilege.Index second = values[randomIntBetween(0, values.length-1)];

        Privilege.Name name = new Privilege.Name(first.name().toString(), second.name().toString());
        Privilege.Index index = Privilege.Index.get(name);

        assertThat(index.implies(first), is(true));
        assertThat(index.implies(second), is(true));

        if (first.implies(second)) {
            assertThat(index, is(first));
        }

        if (second.implies(first)) {
            if (index != second) {
                Privilege.Index idx = Privilege.Index.get(name);
                idx.name().toString();
            }
            assertThat(index, is(second));
        }

        for (Privilege.Index other : Privilege.Index.values()) {
            if (first.implies(other) || second.implies(other) || index.isAlias(other)) {
                assertThat("index privilege [" + index + "] should imply [" + other + "]", index.implies(other), is(true));
            } else if (other.implies(first) && other.implies(second)) {
                assertThat("index privilege [" + index + "] should not imply [" + other + "]", index.implies(other), is(false));
            }
        }
    }
}
