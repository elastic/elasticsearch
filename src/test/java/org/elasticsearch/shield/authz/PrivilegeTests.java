/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.shield.support.Automatons;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class PrivilegeTests extends ElasticsearchTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
    public void testSubActionPattern() throws Exception {
        AutomatonPredicate predicate = new AutomatonPredicate(Automatons.patterns("foo" + Privilege.SUB_ACTION_SUFFIX_PATTERN));
        assertThat(predicate.apply("foo[n][nodes]"), is(true));
        assertThat(predicate.apply("foo[n]"), is(true));
        assertThat(predicate.apply("bar[n][nodes]"), is(false));
        assertThat(predicate.apply("[n][nodes]"), is(false));
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
    public void testCluster_InvalidNaem() throws Exception {
        thrown.expect(ElasticsearchIllegalArgumentException.class);
        Privilege.Name actionName = new Privilege.Name("foobar");
        Privilege.Cluster.get(actionName);
    }

    @Test
    public void testClusterAction() throws Exception {
        Privilege.Name actionName = new Privilege.Name("cluster:admin/snapshot/delete");
        Privilege.Cluster cluster = Privilege.Cluster.get(actionName);
        assertThat(cluster, notNullValue());
        assertThat(cluster.predicate().apply("cluster:admin/snapshot/delete"), is(true));
        assertThat(cluster.predicate().apply("cluster:admin/snapshot/dele"), is(false));
    }

    @Test
    public void testIndexAction() throws Exception {
        Privilege.Name actionName = new Privilege.Name("indices:admin/mapping/delete");
        Privilege.Index index = Privilege.Index.get(actionName);
        assertThat(index, notNullValue());
        assertThat(index.predicate().apply("indices:admin/mapping/delete"), is(true));
        assertThat(index.predicate().apply("indices:admin/mapping/dele"), is(false));
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
