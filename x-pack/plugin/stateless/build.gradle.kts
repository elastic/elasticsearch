import org.elasticsearch.gradle.internal.info.BuildParams

plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "stateless"
    description = "Stateless module for Elasticsearch"
    classname = "co.elastic.elasticsearch.stateless.Stateless"
}

dependencies {
    dependencies {
        compileOnly(xpackModule("core"))
        internalClusterTestImplementation(testArtifact(xpackModule("core")))
    }
}

tasks {
    internalClusterTest {
        systemProperty("es.use_stateless", "true")
        enabled = BuildParams.isSnapshotBuild()
    }
}