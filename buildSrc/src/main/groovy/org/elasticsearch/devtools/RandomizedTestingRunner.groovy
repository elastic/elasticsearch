package org.elasticsearch.devtools

import groovy.xml.NamespaceBuilder;
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskAction;

class RandomizedTestingRunner extends DefaultTask {
    @Input
    SourceSet sourceSet = ((SourceSetContainer)getProject().getProperties().get('sourceSets')).getByName('test')

    @Input
    workingDir = new File(project.buildDir, "run-test")

    @TaskAction
    def run() {
        if (sourceSet == null) {
            sourceSet = ((SourceSetContainer)getProject().getProperties().get('sourceSets')).getByName('test')
        }
        ant.taskdef(resource: 'com/carrotsearch/junit4/antlib.xml',
                uri: 'junit4',
                classpath: getProject().configurations.testCompile.asPath)
        def junit4 = NamespaceBuilder.newInstance(ant, 'junit4')
        junit4.pickseed(property: 'tests.seed')
        junit4.junit4(
                taskName: 'junit4',
                parallelism: 8,
                dir: workingDir) {
            classpath {
                pathElement(path: sourceSet.runtimeClasspath.asPath)
            }
            jvmarg(line: '-ea -esa')
            fileset(dir: sourceSet.output.classesDir) {
                include(name: '**/*IT.class') // temp
                //include(name:'**/*Test.class')
                //include(name:'**/*Tests.class')
                exclude(name: '**/Abstract*.class')
                exclude(name: '**/*StressTest.class')
            }
            listeners {
                junit4.'report-text'(
                        showThrowable: true,
                        showStackTraces: true,
                        showOutput: 'onerror', // TODO: change to property
                        showStatusOk: false,
                        showStatusError: true,
                        showStatusFailure: true,
                        showStatusIgnored: true,
                        showSuiteSummary: true,
                        timestamps: false
                )
            }
        }
    }
}
