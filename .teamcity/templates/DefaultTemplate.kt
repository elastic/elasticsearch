/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package templates

import jetbrains.buildServer.configs.kotlin.v2019_2.Template

object DefaultTemplate : Template({
    params {
        param("JAVA_HOME", "/var/lib/jenkins/.java/openjdk14")
        param("RUNTIME_JAVA_HOME", "/var/lib/jenkins/.java/openjdk11")
        param("JAVA7_HOME", "/var/lib/jenkins/.java/java7")
        param("JAVA8_HOME", "/var/lib/jenkins/.java/java8")
        param("JAVA9_HOME", "/var/lib/jenkins/.java/java9")
        param("JAVA10_HOME", "/var/lib/jenkins/.java/java10")
        param("JAVA11_HOME", "/var/lib/jenkins/.java/java11")
        param("JAVA12_HOME", "/var/lib/jenkins/.java/openjdk12")
        param("JAVA13_HOME", "/var/lib/jenkins/.java/openjdk13")
        param("JAVA14_HOME", "/var/lib/jenkins/.java/openjdk14")
        param("GRADLE_OPTS", "-XX:+HeapDumpOnOutOfMemoryError -Xmx128m -Xms128m")
        param("GRADLEW", "./gradlew --parallel --scan --build-cache -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/")
    }
})
