/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

abstract class AbstractEsqlClientYamlIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected final String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected AbstractEsqlClientYamlIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Before
    @After
    private void assertRequestBreakerEmpty() throws Exception {
        /*
         * This hook is shared by all subclasses. If it is public it we'll
         * get complaints that it is inherited. It isn't. Whatever. Making
         * it private works - the hook still runs. It just looks strange.
         */
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }
}
