/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

/**
 * Runs every {@link ProfileIT} test against a cluster whose {@code my_domain} makes profile uids from the literal
 * username. Both uid modes used to be picked at random per build, which let a build cache hit hide whichever mode had not
 * run. This field hides {@link ProfileIT#cluster}, so JUnit starts only this class's cluster.
 */
public class ProfileLiteralUsernameIT extends ProfileIT {

    @ClassRule
    public static ElasticsearchCluster cluster = createCluster(true);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
