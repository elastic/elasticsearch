import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersTask
import org.elasticsearch.gradle.plugin.BasePluginBuildPlugin
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask
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

    // Use the upstream (non-serverless) integ-test-zip distribution for self-managed REST tests,
    // so that these tests do not depend on the serverless CLI. (see ES-14485)
    // The integ-test-zip is a minimal distribution, so we explicitly list all additional modules
    // needed by the self-managed stateless tests.
    val upstreamIntegTestZip = configurations.create("upstreamIntegTestZip") {
        isCanBeConsumed = false
        isCanBeResolved = true
        attributes {
            attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
        }
    }
    dependencies {
        add("upstreamIntegTestZip", "org.elasticsearch.distribution.integ-test-zip:integ-test-zip:${version}")
    }

    // Collect all module exploded bundles needed for stateless self-managed test clusters.
    // This includes self-managed modules and their upstream dependencies (e.g., blob-cache).
    val selfManagedTestClusterModules = configurations.create("selfManagedTestClusterModules") {
        isCanBeConsumed = false
        isCanBeResolved = true
        attributes {
            attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
            attribute(BasePluginBuildPlugin.EXPLODED_PLUGIN_BUNDLE_ATTRIBUTE, true)
        }
    }
    dependencies {
        // Self-managed modules
        rootProject.project(":modules-self-managed").subprojects.forEach { mod ->
            if (mod.path != ":modules-self-managed:stateless-test-framework" && mod.path != ":modules-self-managed:vector") {
                add("selfManagedTestClusterModules", dependencies.project(
                    mapOf("path" to mod.path, "configuration" to BasePluginBuildPlugin.EXPLODED_BUNDLE_CONFIG)
                ))
            }
        }
        // Upstream modules required by self-managed modules (extended plugins not in integ-test-zip)
        add("selfManagedTestClusterModules", "org.elasticsearch.plugin:blob-cache")
        add("selfManagedTestClusterModules", "org.elasticsearch.plugin:data-streams")
        add("selfManagedTestClusterModules", "org.elasticsearch.plugin:mapper-extras")
        add("selfManagedTestClusterModules", "org.elasticsearch.plugin:logsdb")
    }

    val upstreamIntegTestZipFiles = files(upstreamIntegTestZip)
    val selfManagedTestClusterModulesFiles = files(selfManagedTestClusterModules)

    fun StandaloneRestIntegTestTask.configureForSelfManagedStateless() {
        dependsOn(upstreamIntegTestZip, selfManagedTestClusterModules)
        val nonInputProps = extensions.getByType(SystemPropertyCommandLineArgumentProvider::class.java)
        nonInputProps.systemProperty("tests.integ-test.distribution") { upstreamIntegTestZipFiles.singleFile.path }
        nonInputProps.systemProperty("tests.cluster.modules.path") {
            selfManagedTestClusterModulesFiles.files.joinToString(separator = File.pathSeparator) { it.path }
        }
    }

    pluginManager.withPlugin("elasticsearch.internal-yaml-rest-test") {
        dependencies {
            add("yamlRestTestImplementation", project(":modules-self-managed:stateless-test-framework"))
        }
        tasks.withType<StandaloneRestIntegTestTask>().configureEach {
            configureForSelfManagedStateless()
        }
    }

    pluginManager.withPlugin("elasticsearch.internal-java-rest-test") {
        dependencies {
            add("javaRestTestImplementation", project(":modules-self-managed:stateless-test-framework"))
        }
        tasks.withType<StandaloneRestIntegTestTask>().configureEach {
            configureForSelfManagedStateless()
        }
    }
}
