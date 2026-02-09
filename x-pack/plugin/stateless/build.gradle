import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersTask
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask
import org.elasticsearch.gradle.util.GradleUtils
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
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "stateless"
    description = "Elasticsearch Expanded Pack Plugin - Stateless self managed"
    classname = "org.elasticsearch.xpack.stateless.StatelessPlugin"
    extendedPlugins = listOf("x-pack-core", "blob-cache")
}

// TODO: clean up this file and remove unnecessary stuff for stateless self-managed ES-13786

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
    testImplementation {
        exclude(group = "javax.xml.bind", module = "jaxb-api")
    }
}

GradleUtils.extendSourceSet(project, "main", "javaRestTest", tasks.withType<Test>().named("javaRestTest"))

dependencies {
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("blob-cache"))
    compileOnly(project(":libs:serverless-shared-constants"))
    implementation(project(":libs:serverless-stateless-api"))
    internalClusterTestImplementation(testArtifact(xpackModule("core")))
    internalClusterTestImplementation(xpackModule("shutdown"))
    internalClusterTestImplementation(project(":modules:serverless-autoscaling"))
    internalClusterTestImplementation("org.elasticsearch.plugin:data-streams")
    internalClusterTestImplementation("org.elasticsearch.plugin:mapper-extras")
    internalClusterTestImplementation(project(":modules:serverless-multi-project"))
    internalClusterTestImplementation(project(":modules:secure-settings"))
    internalClusterTestImplementation(xpackModule("esql"))
    internalClusterTestImplementation(xpackModule("esql-core"))
    testImplementation(project(":libs:serverless-shared-constants"))
    testImplementation(testArtifact(xpackModule("searchable-snapshots")))
    testImplementation("com.amazonaws:aws-java-sdk-core:1.12.684")
    testImplementation("com.amazonaws:aws-java-sdk-s3:1.12.684")
    testImplementation("org.elasticsearch.test:s3-fixture")
    testImplementation("org.elasticsearch.test:gcs-fixture")
    testImplementation("org.elasticsearch.test:azure-fixture")
    testImplementation("org.elasticsearch.test:aws-fixture-utils")
    testImplementation("org.elasticsearch.plugin:repository-s3")
    testImplementation("org.elasticsearch.plugin:repository-gcs")
    testImplementation("org.elasticsearch.plugin:repository-azure")
    testImplementation(testArtifact("org.elasticsearch:server"))
    javaRestTestImplementation(project(":libs:serverless-shared-constants"))
}

restResources {
    restApi {
        include("_common", "cluster", "indices", "index", "search")
    }
}

tasks {
    test {
        exclude("**/S3RegisterCASLinearizabilityTests.class")
        // A small writer buffer size so that write into cache region needs to be done in multiple batches
        // This allows reading small files to finish earlier before the region is fully filled which is a behaviour
        // we want to test
        systemProperty("es.searchable.snapshot.shared_cache.write_buffer.size", "8kb")
    }

    internalClusterTest {
        // Tests run in a single classloader as an unnamed module, so the blobcache module
        // is not defined. Here we open java.io to the entire test to quiet spurious
        // warnings about failing to change access for FileDescriptor.fd
        // that org.elasticsearch.preallocate does
        jvmArgs("--add-opens=java.base/java.io=ALL-UNNAMED")
    }

    yamlRestTest {
        usesDefaultDistribution("to be triaged")
    }

    register<Test>("statelessS3ThirdPartyTests") {
        val testSourceSet = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME)
        setTestClassesDirs(testSourceSet.getOutput().getClassesDirs())
        setClasspath(testSourceSet.getRuntimeClasspath())
        include("**/S3RegisterCASLinearizabilityTests.class")
        systemProperty("test.s3.access_key", System.getenv("stateless_aws_s3_access_key"))
        systemProperty("test.s3.secret_key", System.getenv("stateless_aws_s3_secret_key"))
        systemProperty("test.s3.session_token", System.getenv("stateless_aws_s3_session_token"))
        systemProperty("test.s3.bucket", System.getenv("stateless_aws_s3_bucket"))
        systemProperty("test.s3.region", System.getenv("stateless_aws_s3_region"))
        systemProperty("test.s3.base_path", System.getenv("stateless_aws_s3_base_path"))
    }

    javaRestTest {
        usesDefaultDistribution("to be triaged")
    }
}
