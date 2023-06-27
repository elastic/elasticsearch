import org.gradle.api.tasks.SourceSet

plugins {
    id("elasticsearch.internal-es-plugin")
    id("elasticsearch.internal-cluster-test")
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

dependencies {
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("blob-cache"))
    internalClusterTestImplementation(testArtifact(xpackModule("core")))
    testImplementation(testArtifact(xpackModule("searchable-snapshots")))
    testImplementation("com.amazonaws:aws-java-sdk-core")
    testImplementation("org.elasticsearch.test:s3-fixture")
    testImplementation("org.elasticsearch.test:gcs-fixture")
    testImplementation("org.elasticsearch.test:azure-fixture")
    testImplementation("org.elasticsearch.plugin:repository-s3")
    testImplementation("org.elasticsearch.plugin:repository-gcs")
    testImplementation("org.elasticsearch.plugin:repository-azure")
    testImplementation(testArtifact("org.elasticsearch:server"))
}

tasks {
    test {
        exclude("**/S3RegisterCASLinearizabilityTests.class")
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
}
