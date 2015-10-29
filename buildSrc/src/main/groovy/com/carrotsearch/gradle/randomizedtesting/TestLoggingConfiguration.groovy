package com.carrotsearch.gradle.randomizedtesting

import org.gradle.util.ConfigureUtil

class TestLoggingConfiguration {
    SlowTestsConfiguration slowTests = new SlowTestsConfiguration()
    StackTraceFiltersConfiguration stackTraceFilters = new StackTraceFiltersConfiguration()

    void slowTests(Closure closure) {
        ConfigureUtil.configure(closure, slowTests)
    }

    void stackTraceFilters(Closure closure) {
        ConfigureUtil.configure(closure, stackTraceFilters)
    }
}
