/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class IndexResolverTests extends ESTestCase {

    public void testDetermineUnavailableRemoteClusters() {
        // two clusters, both "remote unavailable" type exceptions
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSuchRemoteClusterException("remote2")));
            failures.add(
                new FieldCapabilitiesFailure(
                    new String[] { "remote1:foo", "remote1:bar" },
                    new IllegalStateException("Unable to open any connections")
                )
            );

            Set<String> unavailableClusters = IndexResolver.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters, equalTo(Set.of("remote1", "remote2")));
        }

        // one cluster with "remote unavailable" with two failures
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSuchRemoteClusterException("remote2")));
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSeedNodeLeftException("no seed node")));

            Set<String> unavailableClusters = IndexResolver.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters, equalTo(Set.of("remote2")));
        }

        // two clusters, one "remote unavailable" type exceptions and one with another type
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote1:mylogs1" }, new CorruptIndexException("foo", "bar")));
            failures.add(
                new FieldCapabilitiesFailure(
                    new String[] { "remote2:foo", "remote2:bar" },
                    new IllegalStateException("Unable to open any connections")
                )
            );
            Set<String> unavailableClusters = IndexResolver.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters, equalTo(Set.of("remote2")));
        }

        // one cluster1 with exception not known to indicate "remote unavailable"
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote1:mylogs1" }, new RuntimeException("foo")));
            Set<String> unavailableClusters = IndexResolver.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters, equalTo(Set.of()));
        }

        // empty failures list
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            Set<String> unavailableClusters = IndexResolver.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters, equalTo(Set.of()));
        }
    }
}
