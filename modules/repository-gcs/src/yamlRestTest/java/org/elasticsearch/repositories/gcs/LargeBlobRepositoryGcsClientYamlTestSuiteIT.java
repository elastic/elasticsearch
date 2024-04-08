/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;

public class LargeBlobRepositoryGcsClientYamlTestSuiteIT extends RepositoryGcsClientYamlTestSuiteIT {

    static {
        clusterConfig = c -> c.systemProperty("es.repository_gcs.large_blob_threshold_byte_size", "256");
    }

    public LargeBlobRepositoryGcsClientYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }
}
