package com.carrotsearch.gradle.junit4

import com.carrotsearch.ant.tasks.junit4.SuiteBalancer
import com.carrotsearch.ant.tasks.junit4.balancers.ExecutionTimeBalancer
import com.carrotsearch.ant.tasks.junit4.listeners.ExecutionTimesReport
import org.apache.tools.ant.types.FileSet

class BalancersConfiguration {
    // parent task, so executionTime can register an additional listener
    RandomizedTestingTask task
    List<SuiteBalancer> balancers = new ArrayList<>()

    void executionTime(Map<String,Object> properties) {
        ExecutionTimeBalancer balancer = new ExecutionTimeBalancer()

        FileSet fileSet = new FileSet()
        Object filename = properties.remove('cacheFilename')
        if (filename == null) {
            throw new IllegalArgumentException('cacheFilename is required for executionTime balancer')
        }
        fileSet.setIncludes(filename.toString())

        File cacheDir = task.project.projectDir
        Object dir = properties.remove('cacheDir')
        if (dir != null) {
            cacheDir = new File(dir.toString())
        }
        fileSet.setDir(cacheDir)
        balancer.add(fileSet)

        int historySize = 10
        Object size = properties.remove('historySize')
        if (size instanceof Integer) {
            historySize = (Integer)size
        } else if (size != null) {
            throw new IllegalArgumentException('historySize must be an integer')
        }
        ExecutionTimesReport listener = new ExecutionTimesReport()
        listener.setFile(new File(cacheDir, filename.toString()))
        listener.setHistoryLength(historySize)

        if (properties.isEmpty() == false) {
            throw new IllegalArgumentException('Unknown properties for executionTime balancer: ' + properties.keySet())
        }

        task.listenersConfig.listeners.add(listener)
        balancers.add(balancer)
    }

    void custom(SuiteBalancer balancer) {
        balancers.add(balancer)
    }
}
