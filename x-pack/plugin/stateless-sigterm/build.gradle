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
    id("elasticsearch.internal-es-plugin")
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "stateless-sigterm"
    description = "Sigterm shutdown module for stateless Elasticsearch"
    classname = "org.elasticsearch.xpack.stateless.shutdown.StatelessSigtermPlugin"
    extendedPlugins = listOf("x-pack-core")
    deploymentTarget = "STATELESS_ONLY"
}

dependencies {
    compileOnly(xpackModule("core"))
    implementation(xpackModule("shutdown"))
    javaRestTestImplementation(testArtifact(xpackModule("plugin")))
    javaRestTestImplementation(testArtifact(xpackModule("core")))
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}
