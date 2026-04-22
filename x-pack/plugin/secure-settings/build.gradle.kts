plugins {
    id("elasticsearch.internal-es-plugin")
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "secure-settings"
    description = "Secure Settings module for stateless Elasticsearch"
    classname = "org.elasticsearch.xpack.stateless.settings.secure.SecureSettingsPlugin"
    extendedPlugins = listOf("x-pack-core")
    // Stateless-only for now, but likely going to change
    deploymentTarget = "STATELESS_ONLY"
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    compileOnly(xpackModule("core"))
    testImplementation(testArtifact("org.elasticsearch:server"))
    testImplementation(xpackModule("core"))
}

