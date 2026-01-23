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
    internalClusterTestImplementation(testArtifact(project(":modules-self-managed:stateless"), "internalClusterTest"))
}

// This can be removed once the x-pack plugin is moved to the public repository
tasks.withType<LicenseHeadersTask>().configureEach {
    additionalLicense(
        "ELAST",
        "Elastic License 2.0",
        "2.0; you may not use this file except in compliance with the Elastic License"
    )
    approvedLicenses = listOf("Elastic License 2.0")
}
