import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersTask
import org.gradle.kotlin.dsl.withType

// This folder contains modules related to the self-managed stateless mode of Elasticsearch, that will be
// ultimately moved to the public Elasticsearch repository. (see ES-13955)

plugins {
    id("elasticsearch.internal-es-plugin") apply(false)
}

subprojects {
    if (project.path != ":modules-self-managed:stateless-test-framework"
        && project.path != ":modules-self-managed:vector") {
        apply(plugin = "elasticsearch.internal-es-plugin")
    }

    // Add standard dependencies to all modules
    dependencies {
        if (project.path != ":modules-self-managed:stateless-test-framework"
            && project.path != ":modules-self-managed:vector") {
            add("compileOnly", "org.elasticsearch:server")
            add("testImplementation", "org.elasticsearch.test:framework")
        }
    }

    pluginManager.withPlugin("elasticsearch.internal-yaml-rest-test") {
        dependencies {
            add("yamlRestTestImplementation", project(":modules-self-managed:stateless-test-framework"))
        }
    }

    pluginManager.withPlugin("elasticsearch.internal-java-rest-test") {
        dependencies {
            add("javaRestTestImplementation", project(":modules-self-managed:stateless-test-framework"))
        }
    }
}
