package com.carrotsearch.gradle.junit4

import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import com.carrotsearch.ant.tasks.junit4.listeners.antxml.AntXmlReport


class ListenersConfiguration {
    RandomizedTestingTask task
    List<AggregatedEventListener> listeners = new ArrayList<>()

    void junitReport(Map<String, Object> props) {
        AntXmlReport reportListener = new AntXmlReport()
        Object dir = props == null ? null : props.get('dir')
        if (dir != null) {
            reportListener.setDir(task.project.file(dir))
        } else {
            reportListener.setDir(new File(task.project.buildDir, 'reports' + File.separator + "${task.name}Junit"))
        }
        listeners.add(reportListener)
    }

    void custom(AggregatedEventListener listener) {
        listeners.add(listener)
    }
}
