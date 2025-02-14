/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

public class LogsdbWithBasicRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .systemProperty("es.mapping.synthetic_source_fallback_to_stored_source.cutoff_date_restricted_override", "2027-12-31T23:59")
        .setting("xpack.security.enabled", "false")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCustomCutoffDateUsage() throws IOException {
        var response = getAsMap("/_xpack/usage");
        Map<?, ?> usage = (Map<?, ?>) response.get("logsdb");
        assertThat(usage, Matchers.hasEntry("available", true));
        assertThat(usage, Matchers.hasEntry("enabled", true));
        assertThat(usage, Matchers.hasEntry("indices_count", 0));
        assertThat(usage, Matchers.hasEntry("indices_with_synthetic_source", 0));
        assertThat(usage, Matchers.hasEntry("num_docs", 0));
        assertThat(usage, Matchers.hasEntry("size_in_bytes", 0));
        assertThat(usage, Matchers.hasEntry("has_custom_cutoff_date", true));
    }
}
