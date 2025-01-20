/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.hdfs;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.hdfs.HdfsFixture;
import org.elasticsearch.test.fixtures.krb5kdc.Krb5kDcContainer;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class SecureHaHdfsFailoverTestSuiteIT extends AbstractHaHdfsFailoverTestSuiteIT {

    public static Krb5kDcContainer krb5Fixture = new Krb5kDcContainer();

    public static HdfsFixture hdfsFixture = new HdfsFixture().withHAService("ha-hdfs")
        .withKerberos(() -> krb5Fixture.getPrincipal(), () -> krb5Fixture.getKeytab());

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .plugin("repository-hdfs")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .systemProperty("java.security.krb5.conf", () -> krb5Fixture.getConfPath().toString())
        .configFile("repository-hdfs/krb5.conf", Resource.fromString(() -> krb5Fixture.getConf()))
        .configFile("repository-hdfs/krb5.keytab", Resource.fromFile(() -> krb5Fixture.getEsKeytab()))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(krb5Fixture).around(hdfsFixture).around(cluster);

    @Override
    HdfsFixture getHdfsFixture() {
        return hdfsFixture;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected String securityCredentials() {
        return String.format(java.util.Locale.ROOT, """
            "security.principal": "%s","conf.dfs.data.transfer.protection": "authentication",""", krb5Fixture.getEsPrincipal());
    }

}
