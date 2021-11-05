/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;

import java.text.NumberFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Simple stop watch, allowing for timing of a number of tasks,
 * exposing total running time and running time for each named task.
 * <p>
 * Conceals use of <code>System.nanoTime()</code>, improving the
 * readability of application code and reducing the likelihood of calculation errors.
 * <p>
 * Note that this object is not designed to be thread-safe and does not
 * use synchronization.
 * <p>
 * This class is normally used to verify performance during proof-of-concepts
 * and in development, rather than as part of production applications.
 *
 *
 */
public class StopWatch {

    /**
     * Identifier of this stop watch.
     * Handy when we have output from multiple stop watches
     * and need to distinguish between them in log or console output.
     */
    private final String id;

    private final List<TaskInfo> taskList = new LinkedList<>();

    /**
     * Start time of the current task
     */
    private long startTimeNS;

    /**
     * Is the stop watch currently running?
     */
    private boolean running;

    /**
     * Name of the current task
     */
    private String currentTaskName;

    private TaskInfo lastTaskInfo;

    /**
     * Total running time
     */
    private long totalTimeNS;

    /**
     * Construct a new stop watch. Does not start any task.
     */
    public StopWatch() {
        this.id = "";
    }

    /**
     * Construct a new stop watch with the given id.
     * Does not start any task.
     *
     * @param id identifier for this stop watch.
     *           Handy when we have output from multiple stop watches
     *           and need to distinguish between them.
     */
    public StopWatch(String id) {
        this.id = id;
    }

    /**
     * Start an unnamed task. The results are undefined if {@link #stop()}
     * or timing methods are called without invoking this method.
     *
     * @see #stop()
     */
    public StopWatch start() throws IllegalStateException {
        return start("");
    }

    /**
     * Start a named task. The results are undefined if {@link #stop()}
     * or timing methods are called without invoking this method.
     *
     * @param taskName the name of the task to start
     * @see #stop()
     */
    public StopWatch start(String taskName) throws IllegalStateException {
        if (this.running) {
            throw new IllegalStateException("Can't start StopWatch: it's already running");
        }
        this.startTimeNS = System.nanoTime();
        this.running = true;
        this.currentTaskName = taskName;
        return this;
    }

    /**
     * Stop the current task. The results are undefined if timing
     * methods are called without invoking at least one pair
     * {@link #start()} / {@code #stop()} methods.
     *
     * @see #start()
     */
    public StopWatch stop() throws IllegalStateException {
        if (this.running == false) {
            throw new IllegalStateException("Can't stop StopWatch: it's not running");
        }
        long lastTimeNS = System.nanoTime() - this.startTimeNS;
        this.totalTimeNS += lastTimeNS;
        this.lastTaskInfo = new TaskInfo(this.currentTaskName, TimeValue.nsecToMSec(lastTimeNS));
        this.taskList.add(lastTaskInfo);
        this.running = false;
        this.currentTaskName = null;
        return this;
    }

    public Releasable timing(String taskName) {
        start(taskName);
        return this::stop;
    }

    /**
     * Return whether the stop watch is currently running.
     */
    public boolean isRunning() {
        return this.running;
    }

    /**
     * Return the time taken by the last task.
     */
    public TimeValue lastTaskTime() throws IllegalStateException {
        if (this.lastTaskInfo == null) {
            throw new IllegalStateException("No tests run: can't get last interval");
        }
        return this.lastTaskInfo.getTime();
    }

    /**
     * Return the total time for all tasks.
     */
    public TimeValue totalTime() {
        return new TimeValue(totalTimeNS, TimeUnit.NANOSECONDS);
    }

    /**
     * Return an array of the data for tasks performed.
     */
    public TaskInfo[] taskInfo() {
        return this.taskList.toArray(new TaskInfo[0]);
    }

    /**
     * Return a short description of the total running time.
     */
    public String shortSummary() {
        return "StopWatch '" + this.id + "': running time  = " + totalTime();
    }

    /**
     * Return a string with a table describing all tasks performed.
     * For custom reporting, call getTaskInfo() and use the task info directly.
     */
    public String prettyPrint() {
        StringBuilder sb = new StringBuilder(shortSummary());
        sb.append('\n');
        sb.append("-----------------------------------------\n");
        sb.append("ms     %     Task name\n");
        sb.append("-----------------------------------------\n");
        NumberFormat nf = NumberFormat.getNumberInstance(Locale.ROOT);
        nf.setMinimumIntegerDigits(5);
        nf.setGroupingUsed(false);
        NumberFormat pf = NumberFormat.getPercentInstance(Locale.ROOT);
        pf.setMinimumIntegerDigits(3);
        pf.setGroupingUsed(false);
        for (TaskInfo task : taskInfo()) {
            sb.append(nf.format(task.getTime().millis())).append("  ");
            sb.append(pf.format(task.getTime().secondsFrac() / totalTime().secondsFrac())).append("  ");
            sb.append(task.getTaskName()).append("\n");
        }
        return sb.toString();
    }

    /**
     * Return an informative string describing all tasks performed
     * For custom reporting, call <code>getTaskInfo()</code> and use the task info directly.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(shortSummary());
        for (TaskInfo task : taskInfo()) {
            sb.append("; [").append(task.getTaskName()).append("] took ").append(task.getTime());
            long percent = Math.round((100.0f * task.getTime().millis()) / totalTime().millis());
            sb.append(" = ").append(percent).append("%");
        }
        return sb.toString();
    }

    /**
     * Inner class to hold data about one task executed within the stop watch.
     */
    public static class TaskInfo {

        private final String taskName;

        private final TimeValue timeValue;

        private TaskInfo(String taskName, long timeMillis) {
            this.taskName = taskName;
            this.timeValue = new TimeValue(timeMillis, TimeUnit.MILLISECONDS);
        }

        /**
         * Return the name of this task.
         */
        public String getTaskName() {
            return taskName;
        }

        /**
         * Return the time this task took.
         */
        public TimeValue getTime() {
            return timeValue;
        }
    }

}
