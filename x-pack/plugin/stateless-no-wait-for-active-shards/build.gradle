/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

plugins {
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "stateless-no-wait-for-active-shards"
    description = "Action filter to ignore ?wait_for_active_shards= on indexing requests for stateless Elasticsearch"
    classname = "org.elasticsearch.xpack.stateless.waitforactiveshards.NoWaitForActiveShardsPlugin"
    deploymentTarget = "STATELESS_ONLY"
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    compileOnly("org.elasticsearch:server")
    internalClusterTestImplementation(testArtifact(project(":modules-self-managed:stateless")))
    internalClusterTestImplementation(testArtifact(project(":modules-self-managed:stateless"), "internalClusterTest"))
    javaRestTestImplementation(project(":modules-self-managed:stateless-no-wait-for-active-shards"))
}

