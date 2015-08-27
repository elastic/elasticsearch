package org.elasticsearch.devtools;

import org.gradle.api.*

class RandomizedTesting implements Plugin<Project> {

    void apply(Project project) {
        // hack so check task depends on custom test
        def oldTestTask = project.getTasks().findByPath('test')
        project.getTasks().findByPath('check').dependsOn.remove(oldTestTask)
        project.getTasks().remove(oldTestTask)
        project.getLogger().info('check depends on: ' + project.getTasks().findByName('check').dependsOn)
        def newTestTask = project.tasks.create([name: 'test', type: RandomizedTest, group: 'Verification', description: 'Runs unit tests with JUnit4'])
        project.getTasks().findByPath('check').dependsOn.add(newTestTask)
    }
}
