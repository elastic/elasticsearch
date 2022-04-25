/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.gradle.api.provider.Provider;

public class FipsUtils {

    /**
     * In FIPS mode, we need to explicitly set a FIPS compliant password hashing algorithm.
     * This happens automatically for v8 clusters but not for v7 so we set it here.
     */
    public static void setFipsCompliantHashingAlgorithmOnV7(Provider<ElasticsearchCluster> clusterProvider, Version bwcVersion) {
        ElasticsearchCluster cluster = clusterProvider.get();
        if (BuildParams.isInFipsJvm() && bwcVersion.onOrAfter("7.0.0") && bwcVersion.before("8.0.0")) {
            cluster.setting("xpack.security.authc.password_hashing.algorithm", "pbkdf2_stretch");
        }
    }
}
