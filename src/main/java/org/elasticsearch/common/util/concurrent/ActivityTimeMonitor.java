/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Utility class which efficiently monitors time-limited activities. When
 * start() is called on an ActivityTimeMonitor with a maximum length of time,
 * the status will automatically be updated to OVERRUNNING after the allotted
 * time has expired. If a call is made to checkForTimeout() when the status is
 * OVERRUNING this object will throw a {@link ActivityTimedOutException} Calling
 * stop() will reset the monitor status to inactive. It is only permitted
 * to call start() when inactive so in order to establish a new deadline for an active
 * activity you will need to call stop() first then cal start() with the new deadline. 
 * When start() is called the ActivityTimeMonitor is bound to the current thread via
 * a ThreadLocal and calling getCurrentThreadMonitor() will return the instance
 * associated with the current thread.
 * 
 * A background thread is used to handle all instances and set the OVERRUNING status
 * at the appropriate points
 */
public class ActivityTimeMonitor implements Delayed {

    public enum ActivityStatus {
        INACTIVE, ACTIVE, OVERRUNNING
    }
    private final AtomicReferenceFieldUpdater<ActivityTimeMonitor, ActivityStatus>  STATUS_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ActivityTimeMonitor.class, ActivityStatus.class, "status");
    private volatile ActivityStatus status = ActivityStatus.INACTIVE;
    private long scheduledTimeoutInMilliseconds = Long.MIN_VALUE;
    private final Thread associatedThread;

    private ActivityTimeMonitor() {
        associatedThread = Thread.currentThread();
    }

    public boolean moveToInactive() {
        do {
            ActivityStatus s = status;
            switch (s) {
                case INACTIVE:
                    return false;
                case OVERRUNNING:
                case ACTIVE:
                    if (STATUS_UPDATER.compareAndSet(this, s, ActivityStatus.INACTIVE)) {
                        return true;
                    }
                default:
                    assert false;
            }
        } while (true);
    }

    public void moveToActive() {
        do {
            ActivityStatus s = status;
            switch (s) {
                case ACTIVE:
                    throw new ElasticsearchIllegalArgumentException("Can't move to active - already active");
                case OVERRUNNING:
                    assert false;
                case INACTIVE:
                    if (STATUS_UPDATER.compareAndSet(this, s, ActivityStatus.ACTIVE)) {
                        return;
                    }
                default:
                    assert false;
            }
        } while (true);
    }

    public void moveToOverrun() {
        do {
        ActivityStatus s = status;
            switch (s) {
                case OVERRUNNING:
                    assert false;
                case INACTIVE:
                    return;
                case ACTIVE:
                    if (STATUS_UPDATER.compareAndSet(this, s, ActivityStatus.OVERRUNNING)) {
                        return;
                    }
                default:
                    assert false;
            }
        } while (true);
    }

    public ActivityStatus getStatus() {
        return status;
    }

    public void start(long maxTime) {
        assert status == ActivityStatus.INACTIVE;
        this.scheduledTimeoutInMilliseconds = System.currentTimeMillis() + maxTime;
        moveToActive();
        timeoutMonitorThread.add(this);
        timeoutStateTL.set(this);
    }

    public void stop() {
        if (moveToInactive()) {
            // need to remove the old timeout status from the monitor thread -
            // this will also set status to INACTIVE
            timeoutMonitorThread.remove(this);
        }
    }

    public void checkForTimeout() {
        assert assertThread();
        if (status == ActivityStatus.OVERRUNNING) {
            throw new ActivityTimedOutException("Timed out thread " + Thread.currentThread().getName(), getDelay(TimeUnit.MILLISECONDS));
        }

    }

    private boolean assertThread() {
        assert associatedThread == Thread.currentThread() : "ActivityTimeMonitor is not threadsafe - can't be shared across threads";
        return true;
    }

    // an internal thread that monitors timeout activity
    private static final TimeoutMonitorThread timeoutMonitorThread;
    // Thread-Local holding the current timeout state associated with threads 
    private static final ThreadLocal<ActivityTimeMonitor> timeoutStateTL = new ThreadLocal<ActivityTimeMonitor>();

    static {
        timeoutMonitorThread = new TimeoutMonitorThread();
        timeoutMonitorThread.setDaemon(true);
        timeoutMonitorThread.start();
    }

    /**
     * 
     * @return the number of timed activities currently being monitored
     */
    public static int getCurrentNumActivities() {
        return timeoutMonitorThread.inboundMessageQueue.size();
    }

    /**
     * Get the timeout status information for the current thread
     * 
     * @return null or the current ActivityTimeMonitor for the current thread
     */
    public static ActivityTimeMonitor getCurrentThreadMonitor() {
        return timeoutStateTL.get();
    }
    
    /**
     * Get the timeout status information for the current thread or create if it does not exist
     * 
     * @return the current thread's ActivityTimeMonitor
     */
    public static ActivityTimeMonitor getOrCreateCurrentThreadMonitor() {

        ActivityTimeMonitor result = timeoutStateTL.get();
        if(result==null){
            result=new ActivityTimeMonitor();
            timeoutStateTL.set(result);
        }
        assert result.assertThread();
        return result;
    }    

    @Override
    public int compareTo(Delayed o) {
        ActivityTimeMonitor other = (ActivityTimeMonitor) o;
        return (int) (scheduledTimeoutInMilliseconds - other.scheduledTimeoutInMilliseconds);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long now = System.currentTimeMillis();
        return unit.convert(scheduledTimeoutInMilliseconds - now, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Helper method to start a new activity
     * @param maxTimeInMilliseconds the maximum length of time allowed for an activity
     * @return the existing monitor for the current thread or, a new one if none existed before
     */
    public static ActivityTimeMonitor startActivity(long maxTimeInMilliseconds) {
        ActivityTimeMonitor result = getCurrentThreadMonitor();
        if (result == null) {
            result = new ActivityTimeMonitor();
        }
        result.start(maxTimeInMilliseconds);
        return result;
    }

    /**
     * Helper method called to mark the stopping of a timed activity associated with the current thread
     */
    public static ActivityTimeMonitor stopActivity() {
        ActivityTimeMonitor result = getCurrentThreadMonitor();
        if (result != null) {
            result.stop();
        }
        return result;
    }
    

    // A background thread used to set the timed-out status for threads when
    // they occur
    private static final class TimeoutMonitorThread extends Thread {
        // A queue of Thread statuses sorted by their scheduled timeout time.
        DelayQueue<ActivityTimeMonitor> inboundMessageQueue = new DelayQueue<ActivityTimeMonitor>();

        @Override
        public void run() {
            while (true) {
                try {
                    // Block until the first ActivityTimeMonitor has reached its
                    // timeout then set the volatile boolean indicating an error
                    inboundMessageQueue.take().moveToOverrun();
                } catch (InterruptedException e1) {
                    // Need to keep on trucking
                }
            }
        }

        void remove(ActivityTimeMonitor state) {
            assert state.getStatus() == ActivityStatus.INACTIVE;
            inboundMessageQueue.remove(state);
        }

        void add(ActivityTimeMonitor state) {
            assert state.getStatus() == ActivityStatus.ACTIVE;
            if (state.scheduledTimeoutInMilliseconds <= System.currentTimeMillis()) {
                state.moveToOverrun();
            } else {
                inboundMessageQueue.add(state);
            }
        }
    }


}