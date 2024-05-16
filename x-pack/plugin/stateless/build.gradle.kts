import org.elasticsearch.gradle.internal.test.InternalClusterTestPlugin
import org.elasticsearch.gradle.util.GradleUtils

plugins {
    id("elasticsearch.internal-es-plugin")
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-yaml-rest-test")
    id("elasticsearch.internal-test-artifact")
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "stateless"
    description = "Stateless module for Elasticsearch"
    classname = "co.elastic.elasticsearch.stateless.Stateless"
    extendedPlugins = listOf("x-pack-core", "blob-cache")
}

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
    internalClusterTestImplementation(testArtifact(xpackModule("core")))
    internalClusterTestImplementation(xpackModule("shutdown"))
    internalClusterTestImplementation("org.elasticsearch.plugin:data-streams")
    testImplementation(project(":libs:serverless-shared-constants"))
    testImplementation(testArtifact(xpackModule("searchable-snapshots")))
    testImplementation("com.amazonaws:aws-java-sdk-core")
    testImplementation("org.elasticsearch.test:s3-fixture")
    testImplementation("org.elasticsearch.test:gcs-fixture")
    testImplementation("org.elasticsearch.test:azure-fixture")
    testImplementation("org.elasticsearch.plugin:repository-s3")
    testImplementation("org.elasticsearch.plugin:repository-gcs")
    testImplementation("org.elasticsearch.plugin:repository-azure")
    testImplementation(testArtifact("org.elasticsearch:server"))
    javaRestTestImplementation(project(":libs:serverless-shared-constants"))
}

restResources {
    restApi {
        include("_common", "indices", "index")
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

    /**
     * Same as internalClusterTest but without delayed upload enabled
     * TODO: ES-8317 Remove it by merging into internalClusterTest once BCC changes are deployed to production
     */
    val internalClusterTestWitoutRco = register<Test>("internalClusterTestWitoutRco") {
        val sourceSet = sourceSets.getByName(InternalClusterTestPlugin.SOURCE_SET_NAME)
        setTestClassesDirs(sourceSet.getOutput().getClassesDirs())
        setClasspath(sourceSet.getRuntimeClasspath())
        jvmArgs("-XX:+UseG1GC", "--add-opens=java.base/java.io=ALL-UNNAMED")
        systemProperty("es.test.stateless.upload.delayed", "false")
    }

    check {
        dependsOn(internalClusterTestWitoutRco)
    }

    yamlRestTest {
        usesDefaultDistribution()
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
        usesDefaultDistribution()
    }
}
