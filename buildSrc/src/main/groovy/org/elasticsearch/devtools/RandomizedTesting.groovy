package org.elasticsearch.devtools;

import org.gradle.api.*

class RandomizedTesting implements Plugin<Project> {

    void apply(Project project) {
        // hack so check task depends on custom test
        def oldTestTask = project.getTasks().findByPath('test')
        project.getTasks().findByPath('check').dependsOn.remove(oldTestTask)
        project.getTasks().remove(oldTestTask)
        project.getLogger().info('check depends on: ' + project.getTasks().findByName('check').dependsOn)

        def properties = [
                name: 'test',
                type: RandomizedTest,
                dependsOn: ['classes', 'testClasses'],
                group: 'Verification',
                description: 'Runs unit tests with JUnit4'
        ]
        def newTestTask = project.tasks.create(properties)

        // add back to the check task
        project.getTasks().findByPath('check').dependsOn.add(newTestTask)
    }
}
