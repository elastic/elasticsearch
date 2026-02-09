import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersTask

plugins {
    id("elasticsearch.internal-es-plugin")
    id("elasticsearch.internal-java-rest-test")
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-test-artifact")
}

esplugin {
    name = "stateless-health-shards-availability"
    description = "Overrides Shards Availability indicator with stateless-specific version"
    classname = "org.elasticsearch.xpack.stateless.health.StatelessShardsHealthPlugin"
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    implementation("org.elasticsearch:server")
    internalClusterTestImplementation(testArtifact(project(":modules-self-managed:stateless")))
    internalClusterTestImplementation(testArtifact(project(":modules-self-managed:stateless"), "internalClusterTest"))
}

