package org.elasticsearch.devtools.randomizedtesting

import com.carrotsearch.ant.tasks.junit4.JUnit4
import org.gradle.api.AntBuilder
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.TaskContainer

class RandomizedTestingPlugin implements Plugin<Project> {

    void apply(Project project) {
        // Depends on Java to add
        project.pluginManager.apply('java')

        configureTasks(project.tasks)
        configureAnt(project.ant)
    }

    static void configureTasks(TaskContainer tasks) {
        // hack so check task depends on custom test
        def oldTestTask = tasks.findByPath('test')
        tasks.findByPath('check').dependsOn.remove(oldTestTask)
        tasks.remove(oldTestTask)

        Map properties = [
                name: 'test',
                type: RandomizedTestingTask,
                dependsOn: ['classes', 'testClasses'],
                group: 'Verification',
                description: 'Runs unit tests with JUnit4'
        ]
        RandomizedTestingTask newTestTask = tasks.create(properties)
        newTestTask.sourceSetName = 'test'

        // add back to the check task
        tasks.findByPath('check').dependsOn.add(newTestTask)
    }

    static void configureAnt(AntBuilder ant) {
        ant.project.addTaskDefinition('junit4:junit4', JUnit4.class)
    }
}
