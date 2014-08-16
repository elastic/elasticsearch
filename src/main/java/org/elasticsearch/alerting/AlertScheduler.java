/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleJobFactory;

import java.io.IOException;
import java.util.Date;

public class AlertScheduler extends AbstractLifecycleComponent {

    Scheduler scheduler = null;
    private final AlertManager alertManager;
    private final Client client;
    private final TriggerManager triggerManager;
    private final AlertActionManager actionManager;

    @Inject
    public AlertScheduler(Settings settings, AlertManager alertManager, Client client,
                          TriggerManager triggerManager, AlertActionManager actionManager) {
        super(settings);
        this.alertManager = alertManager;
        this.client = client;
        this.triggerManager = triggerManager;
        this.actionManager = actionManager;
        try {
            SchedulerFactory schFactory = new StdSchedulerFactory();
            scheduler = schFactory.getScheduler();
            scheduler.setJobFactory(new SimpleJobFactory());
        } catch (Throwable t) {
            logger.error("Failed to instantiate scheduler", t);
        }
    }

    public boolean deleteAlertFromSchedule(String alertName) {
        try {
            scheduler.deleteJob(new JobKey(alertName));
            return true;
        } catch (SchedulerException se){
            throw new ElasticsearchException("Failed to remove [" + alertName + "] from the scheduler", se);
        }
    }

    public void clearAlerts() {
        try {
            scheduler.clear();
        } catch (SchedulerException se){
            throw new ElasticsearchException("Failed to clear scheduler", se);
        }
    }

    public void executeAlert(String alertName, JobExecutionContext jobExecutionContext){
        logger.warn("Running [{}]",alertName);
        Alert alert = alertManager.getAlertForName(alertName);

        //@TODO : claim alert
        try {
            XContentBuilder builder = createClampedQuery(jobExecutionContext, alert);
            logger.warn("Running the following query : [{}]", builder.string());

            SearchRequestBuilder srb = client.prepareSearch().setSource(builder);
            String[] indices = alert.indices().toArray(new String[0]);
            if (alert.indices() != null ){
                logger.warn("Setting indices to : " + alert.indices());
                srb.setIndices(indices);
            }
            SearchResponse sr = srb.execute().get();
            logger.warn("Got search response hits : [{}]", sr.getHits().getTotalHits() );
            AlertResult result = new AlertResult(alertName, sr, alert.trigger(), triggerManager.isTriggered(alertName,sr), builder, indices);

            if (result.isTriggered) {
                logger.warn("We have triggered");
                actionManager.doAction(alertName,result);
                logger.warn("Did action !");
            }else{
                logger.warn("We didn't trigger");
            }
            alertManager.updateLastRan(alertName, new DateTime(jobExecutionContext.getFireTime()));
            if (!alertManager.addHistory(alertName, result.isTriggered,
                    new DateTime(jobExecutionContext.getScheduledFireTime()), result.query,
                    result.trigger, result.searchResponse.getHits().getTotalHits(), alert.indices()))
            {
                logger.warn("Failed to store history for alert [{}]", alertName);
            }
        } catch (Exception e) {
            logger.error("Failed execute alert [{}]",e, alert.queryName());
        }
    }

    private XContentBuilder createClampedQuery(JobExecutionContext jobExecutionContext, Alert alert) throws IOException {
        Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
        DateTime clampEnd = new DateTime(scheduledFireTime);
        DateTime clampStart = clampEnd.minusSeconds((int)alert.timePeriod().seconds());
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        builder.field("query");
        builder.startObject();
        builder.field("filtered");
        builder.startObject();
        builder.field("query");
        builder.startObject();
        builder.field("template");
        builder.startObject();
        builder.field("id");
        builder.value(alert.queryName());
        builder.endObject();
        builder.endObject();
        builder.field("filter");
        builder.startObject();
        builder.field("range");
        builder.startObject();
        builder.field("@timestamp");
        builder.startObject();
        builder.field("gte");
        builder.value(clampStart);
        builder.field("lt");
        builder.value(clampEnd);
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public void addAlert(String alertName, Alert alert) {
        JobDetail job = JobBuilder.newJob(org.elasticsearch.alerting.AlertExecutorJob.class).withIdentity(alertName).build();
        job.getJobDataMap().put("manager",this);
        CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                .withSchedule(CronScheduleBuilder.cronSchedule(alert.schedule()))
                .build();
        try {
            logger.warn("Scheduling [{}] with schedule [{}]", alertName, alert.schedule());
            scheduler.scheduleJob(job, cronTrigger);
        } catch (SchedulerException se) {
            logger.error("Failed to schedule job",se);
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.warn("Starting Scheduler");
        try {
            scheduler.start();
        } catch (SchedulerException se){
            logger.error("Failed to start quartz scheduler",se);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        try {
            scheduler.shutdown(true);
        } catch (SchedulerException se){
            logger.error("Failed to stop quartz scheduler",se);
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
