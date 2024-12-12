/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;

public class RepositoryS3RegionalStsClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .configFile(S3Service.CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION, Resource.fromString("""
            Atza|IQEBLjAsAhRFiXuWpUXuRvQ9PZL3GMFcYevydwIUFAHZwXZXXXXXXXXJnrulxKDHwy87oGKPznh0D6bEQZTSCzyoCtL_8S07pLpr0zMbn6w1lfVZKNTBdDans\
            FBmtGnIsIapjI6xKR02Yc_2bQ8LZbUXSGm6Ry6_BG7PrtLZtj_dfCTj92xNGed-CrKqjG7nPBjNIL016GGvuS5gSvPRUxWES3VYfm1wl7WTI7jn-Pcb6M-buCgHhFO\
            zTQxod27L9CqnOLio7N3gZAGpsp6n1-AJBOCJckcyXe2c6uD0srOJeZlKUm2eTDVMf8IehDVI0r1QOnTV6KzzAI3OY87Vd_cVMQ"""))
        .environment("AWS_WEB_IDENTITY_TOKEN_FILE", S3Service.CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION)
        .environment("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/FederatedWebIdentityRole")
        .environment("AWS_ROLE_SESSION_NAME", "sts-fixture-test")
        .environment("AWS_STS_REGIONAL_ENDPOINTS", "regional")
        .environment("AWS_REGION", "ap-southeast-2")
        .build();

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        // Run just the basic sanity test to make sure ES starts up and loads the S3 repository with a regional endpoint without an error.
        // It would be great to make actual requests against a test fixture, but setting the region means using a production endpoint.
        // See #102230 for more details.
        return createParameters(new String[] { "repository_s3/10_basic" });
    }

    public RepositoryS3RegionalStsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
