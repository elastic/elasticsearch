package com.carrotsearch.gradle.randomizedtesting

import com.carrotsearch.ant.tasks.junit4.JUnit4
import org.gradle.api.AntBuilder
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.TaskContainer
import org.gradle.api.tasks.testing.Test

class RandomizedTestingPlugin implements Plugin<Project> {

    void apply(Project project) {
        // Depends on Java to add
        project.pluginManager.apply('java')

        replaceTestTask(project.tasks)
        configureAnt(project.ant)
    }

    static void replaceTestTask(TaskContainer tasks) {
        Test oldTestTask = tasks.findByPath('test')
        tasks.remove(oldTestTask)

        Map properties = [
            name: 'test',
            type: RandomizedTestingTask,
            dependsOn: oldTestTask.dependsOn,
            group: 'Verification',
            description: 'Runs unit tests with JUnit4'
        ]
        RandomizedTestingTask newTestTask = tasks.create(properties)
        newTestTask.classpath = oldTestTask.classpath
        newTestTask.testClassesDir = oldTestTask.testClassesDir

        // hack so check task depends on custom test
        Task checkTask = tasks.findByPath('check')
        checkTask.dependsOn.remove(oldTestTask)
        checkTask.dependsOn.add(newTestTask)
    }

    static void configureAnt(AntBuilder ant) {
        ant.project.addTaskDefinition('junit4:junit4', JUnit4.class)
    }
}
