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

        // The following check prevents the addition of new transport versions to self-managed modules until migrated to the public repository.
        // Once all of the modules are migrated, this check is obsolete; this should be the case for the entire directory modules-self-managed.
        val projectPath = project.path
        tasks.named<org.elasticsearch.gradle.internal.transport.CollectTransportVersionReferencesTask>("collectTransportVersionReferences") {
            doLast {
                // Permit existing TVs until cleaned up after the next promotion
                val allowedVersions = setOf("fetch_search_shard_info_fix_serialization")
                val refsFile = outputFile.get().asFile
                if (refsFile.exists()) {
                    val newVersions = refsFile.readLines()
                        .filter { it.isNotBlank() }
                        .map { it.substringBefore(",") }
                        .filter { it !in allowedVersions }
                    if (newVersions.isNotEmpty()) {
                        throw GradleException(
                            "New transport version references were found in $projectPath: ${newVersions.joinToString(", ")}.\n" +
                            "Self-managed modules must not define their own transport versions until moved to the core Elasticsearch repository.\n" +
                            "In the meanwhile, you may introduce necessary transport versions in the Elasticsearch repo using a linked PR."
                        )
                    }
                }
            }
        }
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
