package org.elasticsearch.devtools.randomizedtesting

import org.gradle.api.*
import org.gradle.api.tasks.TaskContainer

class RandomizedTestingPlugin implements Plugin<Project> {

    void apply(Project project) {
        // Depends on Java to add
        project.pluginManager.apply('java')

        configureTasks(project.tasks)
    }

    static void configureTasks(TaskContainer tasks) {
        // hack so check task depends on custom test
        def oldTestTask = tasks.findByPath('test')
        tasks.findByPath('check').dependsOn.remove(oldTestTask)
        tasks.remove(oldTestTask)

        def properties = [
                name: 'test',
                type: RandomizedTestingTask,
                dependsOn: ['classes', 'testClasses'],
                group: 'Verification',
                description: 'Runs unit tests with JUnit4'
        ]
        def newTestTask = tasks.create(properties)

        // add back to the check task
        tasks.findByPath('check').dependsOn.add(newTestTask)
    }
}
