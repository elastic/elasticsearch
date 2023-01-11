import org.elasticsearch.gradle.internal.test.InternalClusterTestPlugin

plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "stateless"
    description = "Stateless module for Elasticsearch"
    classname = "co.elastic.elasticsearch.stateless.Stateless"
    extendedPlugins = listOf("x-pack-core")
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
    dependencies {
        compileOnly(xpackModule("core"))
        internalClusterTestImplementation(testArtifact(xpackModule("core")))
        add("testImplementation", "org.elasticsearch.test:s3-fixture")
        add("testImplementation", "org.elasticsearch.test:gcs-fixture")
        add("testImplementation", "org.elasticsearch.test:azure-fixture")
        add("testImplementation", "com.amazonaws:aws-java-sdk-core")
        add("testImplementation", "org.elasticsearch.plugin:repository-s3")
        add("testImplementation", "org.elasticsearch.plugin:repository-gcs")
        add("testImplementation", "org.elasticsearch.plugin:repository-azure")
    }
}

tasks {
    internalClusterTest {
        exclude ("**/S3RegisterCASLinearizabilityTests.class")
    }
}

tasks.register<Test>("statelessS3ThirdPartyTests") {
  val internalTestSourceSet = sourceSets.getByName(InternalClusterTestPlugin.SOURCE_SET_NAME)
  setTestClassesDirs(internalTestSourceSet.getOutput().getClassesDirs())
  setClasspath(internalTestSourceSet.getRuntimeClasspath())
  include ("**/S3RegisterCASLinearizabilityTests.class")
  systemProperty ("test.s3.access_key", System.getenv("stateless_aws_s3_access_key"))
  systemProperty ("test.s3.secret_key", System.getenv("stateless_aws_s3_secret_key"))
  systemProperty ("test.s3.bucket", System.getenv("stateless_aws_s3_bucket"))
  systemProperty ("test.s3.region", System.getenv("stateless_aws_s3_region"))
  systemProperty ("test.s3.base_path", System.getenv("stateless_aws_s3_base_path"))
}
