/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.util;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * Test for {@link StopWatch}.
 *
 * @author kimchy (Shay Banon)
 */
public class StopWatchTests {

    /**
     * Are timings off in JUnit?
     */
    @Test public void testValidUsage() throws Exception {
        StopWatch sw = new StopWatch();
        long int1 = 166L;
        long int2 = 45L;
        String name1 = "Task 1";
        String name2 = "Task 2";

        long fudgeFactor = 5L;
        assertThat(sw.isRunning(), equalTo(false));
        sw.start(name1);
        Thread.sleep(int1);
        assertThat(sw.isRunning(), equalTo(true));
        sw.stop();

        // TODO are timings off in JUnit? Why do these assertions sometimes fail
        // under both Ant and Eclipse?

        //assertTrue("Unexpected timing " + sw.getTotalTime(), sw.getTotalTime() >= int1);
        //assertTrue("Unexpected timing " + sw.getTotalTime(), sw.getTotalTime() <= int1 + fudgeFactor);
        sw.start(name2);
        Thread.sleep(int2);
        sw.stop();
        //assertTrue("Unexpected timing " + sw.getTotalTime(), sw.getTotalTime() >= int1 + int2);
        //assertTrue("Unexpected timing " + sw.getTotalTime(), sw.getTotalTime() <= int1 + int2 + fudgeFactor);

        assertThat(sw.taskCount(), equalTo(2));
        String pp = sw.prettyPrint();
        assertThat(pp.indexOf(name1) != -1, equalTo(true));
        assertThat(pp.indexOf(name2) != -1, equalTo(true));

        StopWatch.TaskInfo[] tasks = sw.taskInfo();
        assertThat(tasks.length, equalTo(2));
        assertThat(tasks[0].getTaskName(), equalTo(name1));
        assertThat(tasks[1].getTaskName(), equalTo(name2));
        sw.toString();
    }

    @Test public void testValidUsageNotKeepingTaskList() throws Exception {
        StopWatch sw = new StopWatch().keepTaskList(false);
        long int1 = 166L;
        long int2 = 45L;
        String name1 = "Task 1";
        String name2 = "Task 2";

        long fudgeFactor = 5L;
        assertThat(sw.isRunning(), equalTo(false));
        sw.start(name1);
        Thread.sleep(int1);
        assertThat(sw.isRunning(), equalTo(true));
        sw.stop();

        // TODO are timings off in JUnit? Why do these assertions sometimes fail
        // under both Ant and Eclipse?

        //assertTrue("Unexpected timing " + sw.getTotalTime(), sw.getTotalTime() >= int1);
        //assertTrue("Unexpected timing " + sw.getTotalTime(), sw.getTotalTime() <= int1 + fudgeFactor);
        sw.start(name2);
        Thread.sleep(int2);
        sw.stop();
        //assertTrue("Unexpected timing " + sw.getTotalTime(), sw.getTotalTime() >= int1 + int2);
        //assertTrue("Unexpected timing " + sw.getTotalTime(), sw.getTotalTime() <= int1 + int2 + fudgeFactor);

        assertThat(sw.taskCount(), equalTo(2));
        String pp = sw.prettyPrint();
        assertThat(pp.indexOf("kept"), not(equalTo(-1)));
        sw.toString();

        try {
            sw.taskInfo();
            assertThat(false, equalTo(true));
        } catch (UnsupportedOperationException ex) {
            // Ok
        }
    }

    @Test public void testFailureToStartBeforeGettingTimings() {
        StopWatch sw = new StopWatch();
        try {
            sw.lastTaskTime();
            assertThat("Can't get last interval if no tests run", false, equalTo(true));
        } catch (IllegalStateException ex) {
            // Ok
        }
    }

    @Test public void testFailureToStartBeforeStop() {
        StopWatch sw = new StopWatch();
        try {
            sw.stop();
            assertThat("Can't stop without starting", false, equalTo(true));
        } catch (IllegalStateException ex) {
            // Ok
        }
    }

    @Test public void testRejectsStartTwice() {
        StopWatch sw = new StopWatch();
        try {
            sw.start("");
            sw.stop();
            sw.start("");
            assertThat(sw.isRunning(), equalTo(true));
            sw.start("");
            assertThat("Can't start twice", false, equalTo(true));
        } catch (IllegalStateException ex) {
            // Ok
        }
    }
}
