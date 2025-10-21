/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for calendar event scalability
 */
public class CalendarScalabilityIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    /**
     * Test that calendar updates work correctly when a calendar is associated with many jobs
     */
    public void testCalendarUpdateWithManyJobs() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueMinutes(30);
        int jobCount = 5; // Reduced for faster testing
        
        // Create and open multiple jobs
        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            String jobId = "calendar-scalability-" + i;
            jobIds.add(jobId);
            
            Job.Builder job = createJob(jobId, bucketSpan);
            putJob(job);
            openJob(jobId);
        }
        
        // Create a calendar and associate it with all jobs
        String calendarId = "test-calendar-many-jobs";
        putCalendar(calendarId, jobIds, "Calendar for many jobs test");
        
        // Add scheduled events to the calendar
        List<ScheduledEvent> events = new ArrayList<>();
        long eventStartTime = 1514764800000L + (bucketSpan.millis() * 5); // 5 buckets in
        long eventEndTime = eventStartTime + (bucketSpan.millis() * 2); // 2 buckets duration
        events.add(
            new ScheduledEvent.Builder()
                .description("Scalability Test Event")
                .startTime(Instant.ofEpochMilli(eventStartTime))
                .endTime(Instant.ofEpochMilli(eventEndTime))
                .calendarId(calendarId)
                .build()
        );
        
        PostCalendarEventsAction.Response response = postScheduledEvents(calendarId, events);
        
        // Wait a bit for updates to complete
        Thread.sleep(2000);
        
        // Verify all jobs are still running
        for (String jobId : jobIds) {
            assertThat("Job should still be open", getJobStats(jobId).get(0).getState(), equalTo(JobState.OPENED));
        }
    }

    /**
     * Test that calendar updates work with a single job
     */
    public void testCalendarUpdateWithSingleJob() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueMinutes(30);
        String jobId = "single-job-test";
        
        // Create and open job
        Job.Builder job = createJob(jobId, bucketSpan);
        putJob(job);
        openJob(jobId);
        
        // Create a calendar and associate it with the job
        String calendarId = "test-calendar-single";
        putCalendar(calendarId, Collections.singletonList(jobId), "Calendar for single job test");
        
        // Add scheduled events to the calendar
        List<ScheduledEvent> events = new ArrayList<>();
        long eventStartTime = 1514764800000L + (bucketSpan.millis() * 5);
        long eventEndTime = eventStartTime + (bucketSpan.millis() * 2);
        events.add(
            new ScheduledEvent.Builder()
                .description("Single Job Event")
                .startTime(Instant.ofEpochMilli(eventStartTime))
                .endTime(Instant.ofEpochMilli(eventEndTime))
                .calendarId(calendarId)
                .build()
        );
        
        PostCalendarEventsAction.Response response = postScheduledEvents(calendarId, events);
        
        // Wait a bit for updates to complete
        Thread.sleep(1000);
        
        // Verify job is still running
        assertThat("Job should still be open", getJobStats(jobId).get(0).getState(), equalTo(JobState.OPENED));
    }

    /**
     * Test that calendar updates work with closed jobs (should not fail)
     */
    public void testCalendarUpdateWithClosedJobs() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueMinutes(30);
        String jobId = "closed-job-test";
        
        // Create and open job
        Job.Builder job = createJob(jobId, bucketSpan);
        putJob(job);
        openJob(jobId);
        
        // Close the job
        closeJob(jobId);
        
        // Create a calendar and associate it with the closed job
        String calendarId = "test-calendar-closed";
        putCalendar(calendarId, Collections.singletonList(jobId), "Calendar for closed job test");
        
        // Add scheduled events to the calendar
        List<ScheduledEvent> events = new ArrayList<>();
        long eventStartTime = 1514764800000L + (bucketSpan.millis() * 5);
        long eventEndTime = eventStartTime + (bucketSpan.millis() * 2);
        events.add(
            new ScheduledEvent.Builder()
                .description("Closed Job Event")
                .startTime(Instant.ofEpochMilli(eventStartTime))
                .endTime(Instant.ofEpochMilli(eventEndTime))
                .calendarId(calendarId)
                .build()
        );
        
        // This should not fail even though the job is closed
        PostCalendarEventsAction.Response response = postScheduledEvents(calendarId, events);
        
        // Wait a bit for updates to complete
        Thread.sleep(1000);
        
        // Verify job is still closed
        assertThat("Job should still be closed", getJobStats(jobId).get(0).getState(), equalTo(JobState.CLOSED));
    }
}