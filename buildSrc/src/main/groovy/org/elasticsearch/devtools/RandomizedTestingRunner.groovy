package org.elasticsearch.devtools;

import org.gradle.api.*

class RandomizedTestingRunner implements Plugin<Project> {

    void apply(Project project) {
        // hack so check task depends on custom test
        project.getTasks().findByPath("check").dependsOn.remove("test")
        project.getTasks().create([name: 'test', type: RandomizedTest, overwrite: true, group: "Verification"])
        project.getTasks().findByPath("check").dependsOn.add("test")
    }
}
