/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

import org.elasticsearch.gradle.internal.test.TestUtil

plugins {
    id("elasticsearch.java-base")
    id("java-library")
    id("application")
}

dependencies {
    // The elasticsearch modules-self-managed:vector module (KnnIndexTester and friends)
    api("org.elasticsearch:vector")
    // The stateless test artifact (StatelessDirectoryFactory, StatelessDirectory)
    api(project(":modules-self-managed:stateless")) {
        capabilities {
            requireCapability("elasticsearch-serverless.modules-self-managed:stateless-test-artifacts")
        }
    }
}

application {
    mainClass = "org.elasticsearch.test.knn.StatelessKnnIndexTester"
}

tasks {
    assemble {
        enabled = false
    }
    test {
        enabled = false
    }
    javadoc {
        enabled = false
    }
}

tasks.register("printClasspath") {
    group = "Help"
    description = "Prints the classpath needed to run StatelessKnnIndexTester directly with java"
    val cp = sourceSets["main"].runtimeClasspath
    val outputFile = layout.buildDirectory.file("vector_classpath.txt")

    inputs.files(cp)
    outputs.file(outputFile)

    doLast {
        val f = outputFile.get().asFile
        f.parentFile.mkdirs()
        f.writeText(cp.asPath)
        println("Classpath written to: ${f.absolutePath}")
    }
}

tasks.register<JavaExec>("checkVec") {
    group = "Execution"
    description = "Runs KnnIndexTester with the stateless SearchDirectory."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.elasticsearch.test.knn.StatelessKnnIndexTester")
    systemProperty("es.logger.out", "console")
    systemProperty("es.logger.level", "INFO")
    systemProperty(
        "es.nativelibs.path",
        TestUtil.getTestLibraryPath(file("../../elasticsearch/libs/native/libraries/build/platform/").toString())
    )
    jvmArgs(
        "-Xms16g", "-Xmx16g",
        "-Djava.util.concurrent.ForkJoinPool.common.parallelism=8",
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+DebugNonSafepoints",
        "-XX:+HeapDumpOnOutOfMemoryError"
    )
    if (buildParams.runtimeJavaVersion.map { it.majorVersion.toInt() }.get() >= 21) {
        jvmArgs("--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED")
    }
    if (System.getenv("DO_PROFILING") != null) {
        jvmArgs("-XX:StartFlightRecording=dumponexit=true,maxsize=250M,filename=knn.jfr,settings=profile.jfc")
    }
    executable("${buildParams.runtimeJavaHome.get()}/bin/java")
}
