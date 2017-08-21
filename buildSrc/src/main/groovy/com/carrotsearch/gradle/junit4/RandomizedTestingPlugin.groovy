package com.carrotsearch.gradle.junit4

import com.carrotsearch.ant.tasks.junit4.JUnit4
import org.gradle.api.AntBuilder
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.TaskContainer
import org.gradle.api.tasks.testing.Test

class RandomizedTestingPlugin implements Plugin<Project> {

    void apply(Project project) {
        replaceTestTask(project.tasks)
        configureAnt(project.ant)
    }

    static void replaceTestTask(TaskContainer tasks) {
        Test oldTestTask = tasks.findByPath('test')
        if (oldTestTask == null) {
            // no test task, ok, user will use testing task on their own
            return
        }
        tasks.remove(oldTestTask)

        Map properties = [
            name: 'test',
            type: RandomizedTestingTask,
            dependsOn: oldTestTask.dependsOn,
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs unit tests with the randomized testing framework'
        ]
        RandomizedTestingTask newTestTask = tasks.create(properties)
        newTestTask.classpath = oldTestTask.classpath
        newTestTask.testClassesDir = oldTestTask.project.sourceSets.test.output.classesDir

        // hack so check task depends on custom test
        Task checkTask = tasks.findByPath('check')
        checkTask.dependsOn.remove(oldTestTask)
        checkTask.dependsOn.add(newTestTask)
    }

    static void configureAnt(AntBuilder ant) {
        ant.project.addTaskDefinition('junit4:junit4', JUnit4.class)
    }
}
