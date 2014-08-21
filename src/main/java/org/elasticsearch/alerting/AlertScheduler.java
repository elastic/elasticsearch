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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleJobFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AlertScheduler extends AbstractLifecycleComponent {

    Scheduler scheduler = null;
    private final AlertManager alertManager;
    private final Client client;
    private final TriggerManager triggerManager;
    private final AlertActionManager actionManager;
    private final ScriptService scriptService;


    @Inject
    public AlertScheduler(Settings settings, AlertManager alertManager, Client client,
                          TriggerManager triggerManager, AlertActionManager actionManager,
                          ScriptService scriptService) {
        super(settings);
        this.alertManager = alertManager;
        this.client = client;
        this.triggerManager = triggerManager;
        this.actionManager = actionManager;
        this.scriptService = scriptService;
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
        DateTime scheduledTime =  new DateTime(jobExecutionContext.getScheduledFireTime());
        if (!alert.enabled()) {
            logger.warn("Alert [{}] is not enabled", alertName);
            return;
        }
        try {
            if (!alertManager.claimAlertRun(alertName, scheduledTime) ){
                logger.warn("Another process has already run this alert.");
                return;
            }
            alert = alertManager.getAlertForName(alertName); //The claim may have triggered a refresh

            SearchRequestBuilder srb = createClampedRequest(client, jobExecutionContext, alert);
            String[] indices = alert.indices().toArray(new String[0]);

            if (alert.indices() != null ){
                logger.warn("Setting indices to : " + alert.indices());
                srb.setIndices(indices);
            }

            //if (logger.isDebugEnabled()) {
            logger.warn("Running query [{}]", XContentHelper.convertToJson(srb.request().source(), false, true));
            //}

            SearchResponse sr = srb.execute().get();
            logger.warn("Got search response hits : [{}]", sr.getHits().getTotalHits() );
            AlertResult result = new AlertResult(alertName, sr, alert.trigger(),
                    triggerManager.isTriggered(alertName,sr), srb, indices,
                    new DateTime(jobExecutionContext.getScheduledFireTime()));

            boolean firedAction = false;
            if (result.isTriggered) {
                logger.warn("We have triggered");
                DateTime lastActionFire = alertManager.timeActionLastTriggered(alertName);
                long msSinceLastAction = scheduledTime.getMillis() - lastActionFire.getMillis();
                logger.error("last action fire [{}]", lastActionFire);
                logger.error("msSinceLastAction [{}]", msSinceLastAction);

                if (alert.timePeriod().getMillis() > msSinceLastAction) {
                    logger.warn("Not firing action because it was fired in the timePeriod");
                } else {
                    actionManager.doAction(alertName, result);
                    logger.warn("Did action !");
                    firedAction = true;
                }

            } else {
                logger.warn("We didn't trigger");
            }
            alertManager.updateLastRan(alertName, new DateTime(jobExecutionContext.getFireTime()),scheduledTime,firedAction);
            if (!alertManager.addHistory(alertName, result.isTriggered,
                    new DateTime(jobExecutionContext.getScheduledFireTime()), result.query,
                    result.trigger, result.searchResponse.getHits().getTotalHits(), alert.actions(), alert.indices()))
            {
                logger.warn("Failed to store history for alert [{}]", alertName);
            }
        } catch (Exception e) {
            logger.error("Failed execute alert [{}]", e, alertName);
        }
    }

    private SearchRequestBuilder createClampedRequest(Client client, JobExecutionContext jobExecutionContext, Alert alert){
        Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
        DateTime clampEnd = new DateTime(scheduledFireTime);
        DateTime clampStart = clampEnd.minusSeconds((int)alert.timePeriod().seconds());
        if (alert.simpleQuery()) {
            TemplateQueryBuilder queryBuilder = new TemplateQueryBuilder(alert.queryName(), ScriptService.ScriptType.INDEXED, new HashMap<String, Object>());
            RangeFilterBuilder filterBuilder = new RangeFilterBuilder(alert.timestampString());
            filterBuilder.gte(clampStart);
            filterBuilder.lt(clampEnd);
            return client.prepareSearch().setQuery(new FilteredQueryBuilder(queryBuilder, filterBuilder));
        } else {
            //We can't just wrap the template here since it probably contains aggs or something else that doesn't play nice with FilteredQuery
            Map<String,Object> fromToMap = new HashMap<>();
            fromToMap.put("from", clampStart); //@TODO : make these parameters configurable ? Don't want to bloat the API too much tho
            fromToMap.put("to", clampEnd);
            //Go and get the search template from the script service :(
            ExecutableScript script =  scriptService.executable("mustache", alert.queryName(), ScriptService.ScriptType.INDEXED, fromToMap);
            BytesReference requestBytes = (BytesReference)(script.run());
            return client.prepareSearch().setSource(requestBytes);
        }
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
