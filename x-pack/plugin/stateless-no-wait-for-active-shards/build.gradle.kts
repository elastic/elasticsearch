plugins {
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "serverless-no-wait-for-active-shards"
    description = "Action filter to ignore ?wait_for_active_shards= on indexing requests for serverless Elasticsearch"
    classname = "co.elastic.elasticsearch.api.waitforactiveshards.NoWaitForActiveShardsPlugin"
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    compileOnly("org.elasticsearch:server")
    internalClusterTestImplementation(testArtifact(project(":modules:stateless"), "internalClusterTest"))
    javaRestTestImplementation(project(":modules:serverless-no-wait-for-active-shards"))
}

tasks {
    javaRestTest {
        usesDefaultDistribution()
    }
}
