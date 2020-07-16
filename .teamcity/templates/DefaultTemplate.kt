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

import DefaultRoot
import jetbrains.buildServer.configs.kotlin.v2019_2.Template
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.placeholder
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.script

object DefaultTemplate : Template({
    name = "Default Template"

    vcs {
        root(DefaultRoot)

        checkoutDir = "/dev/shm/%system.teamcity.buildType.id%/%system.build.number%"
    }

    params {
        param("gradle.max.workers", "2")
        param("gradle.params", "--max-workers=%gradle.max.workers% --scan --build-cache -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/")

        param("env.JAVA_HOME", "/var/lib/jenkins/.java/openjdk14")
        param("env.RUNTIME_JAVA_HOME", "/var/lib/jenkins/.java/openjdk11")
        param("env.JAVA7_HOME", "/var/lib/jenkins/.java/java7")
        param("env.JAVA8_HOME", "/var/lib/jenkins/.java/java8")
        param("env.JAVA9_HOME", "/var/lib/jenkins/.java/java9")
        param("env.JAVA10_HOME", "/var/lib/jenkins/.java/java10")
        param("env.JAVA11_HOME", "/var/lib/jenkins/.java/java11")
        param("env.JAVA12_HOME", "/var/lib/jenkins/.java/openjdk12")
        param("env.JAVA13_HOME", "/var/lib/jenkins/.java/openjdk13")
        param("env.JAVA14_HOME", "/var/lib/jenkins/.java/openjdk14")
        param("env.GRADLE_OPTS", "-XX:+HeapDumpOnOutOfMemoryError -Xmx128m -Xms128m")

        // For now these are just to ensure compatibility with existing Jenkins-based configuration
        param("env.JENKINS_URL", "%teamcity.serverUrl%")
        param("env.BUILD_URL", "%teamcity.serverUrl%/build/%teamcity.build.id%")
        param("env.JOB_NAME", "%system.teamcity.buildType.id%")
        param("env.GIT_BRANCH", "%vcsroot.branch%")
    }

    steps {
        script {
            name = "Setup Build Environment"

            conditions {
                contains("teamcity.agent.jvm.os.name", "Linux")
            }

            scriptContent = """
                #!/usr/bin/env bash
                # drop page cache and kernel slab objects on linux
                [[ -x /usr/local/sbin/drop-caches ]] && sudo /usr/local/sbin/drop-caches
                rm -Rfv ~/.gradle/init.d
                mkdir -p ~/.gradle/init.d && cp -v .ci/teamcity.init.gradle ~/.gradle/init.d
                if [ -f /proc/cpuinfo ] ; then
                   MAX_WORKERS=`grep '^cpu\scores' /proc/cpuinfo  | uniq | sed 's/\s\+//g' |  cut -d':' -f 2`
                else
                   if [[ "${'$'}OSTYPE" == "darwin"* ]]; then
                      MAX_WORKERS=`sysctl -n hw.physicalcpu | sed 's/\s\+//g'`
                   else
                      echo "Unsupported OS Type:${'$'}OSTYPE"
                      exit 1
                   fi
                fi
                if pwd | grep -v -q ^/dev/shm ; then
                   echo "Not running on a ramdisk, reducing number of workers"
                   MAX_WORKERS=${'$'}((${'$'}MAX_WORKERS*2/3))
                fi

                echo "##teamcity[setParameter name='gradle.max.workers' value='${'$'}MAX_WORKERS']"
            """.trimIndent()
        }
        placeholder {  }
    }
})
