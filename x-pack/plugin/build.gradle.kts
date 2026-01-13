// This folder contains modules related to the self-managed stateless mode of Elasticsearch, that will be
// ultimately moved to the public Elasticsearch repository. (see ES-13955)

plugins {
    id("elasticsearch.internal-es-plugin") apply(false)
}

subprojects {
    apply(plugin = "elasticsearch.internal-es-plugin")

    // Add standard dependencies to all modules
    dependencies {
        add("compileOnly", "org.elasticsearch:server")
        add("testImplementation", "org.elasticsearch.test:framework")
    }
}
