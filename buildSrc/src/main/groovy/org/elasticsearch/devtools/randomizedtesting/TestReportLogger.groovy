package org.elasticsearch.devtools.randomizedtesting

import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import com.carrotsearch.ant.tasks.junit4.listeners.TextReport
import org.gradle.api.logging.Logger

class TestReportLogger implements AggregatedEventListener {
    Logger logger
    TextReport delegate


}
