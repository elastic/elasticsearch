import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersTask
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask
import org.gradle.kotlin.dsl.invoke
import org.gradle.kotlin.dsl.project
import org.gradle.kotlin.dsl.withType
import org.gradle.kotlin.dsl.yamlRestTest

// WARNING: this plugin will be ultimately moved to the public elasticsearch repository as an x-pack plugin. Please
// avoid using a lot of Kotlin functionality from the serverless repository, to ease the ultimate translation
// to gradle when it is moved to the public repository.

plugins {
    id("elasticsearch.internal-es-plugin")
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-yaml-rest-test")
    id("elasticsearch.internal-test-artifact")
}

esplugin {
    name = "stateless"
    description = "Elasticsearch Expanded Pack Plugin - Stateless self managed"
    classname = "org.elasticsearch.xpack.stateless.StatelessPlugin"
    extendedPlugins = listOf("x-pack-core", "blob-cache")
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("blob-cache"))
    testImplementation(testArtifact(xpackModule("core")))
    testImplementation(testArtifact(xpackModule("blob-cache")))
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

tasks {
    yamlRestTest {
        systemProperty("yaml.rest.tests.set_num_nodes", "false")
        // The ILM plugin is not included in serverless, so disabling the ILM history store through a setting would cause an error.
        systemProperty("yaml.rest.tests.disable_ilm_history", "false")
    }
}
