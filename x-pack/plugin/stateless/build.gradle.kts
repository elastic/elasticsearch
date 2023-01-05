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
