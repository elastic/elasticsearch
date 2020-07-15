import jetbrains.buildServer.configs.kotlin.v2019_2.BuildType
import jetbrains.buildServer.configs.kotlin.v2019_2.FailureAction
import jetbrains.buildServer.configs.kotlin.v2019_2.ReuseBuilds
import jetbrains.buildServer.configs.kotlin.v2019_2.SnapshotDependency

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

fun BuildType.dependsOn(buildType: BuildType, init: SnapshotDependency.() -> Unit) {
    dependencies {
        snapshot(buildType) {
            reuseBuilds = ReuseBuilds.SUCCESSFUL
            onDependencyCancel = FailureAction.CANCEL
            onDependencyFailure = FailureAction.CANCEL
            synchronizeRevisions = true
            init()
        }
    }
}

fun BuildType.dependsOn(vararg buildTypes: BuildType, init: SnapshotDependency.() -> Unit) {
    buildTypes.forEach { dependsOn(it, init) }
}

fun BuildType.dependsOn(buildType: BuildType) {
    dependsOn(buildType) {}
}
