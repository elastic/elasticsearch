/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleJobFactory;

import java.util.HashMap;

public class AlertScheduler extends AbstractLifecycleComponent {

    Scheduler scheduler = null;
    private final AlertManager alertManager;
    private final Client client;
    private final ScriptService scriptService;
    private final TriggerManager triggerManager;
    private final AlertActionManager actionManager;

    @Inject
    public AlertScheduler(Settings settings, AlertManager alertManager, ScriptService scriptService, Client client,
                          TriggerManager triggerManager, AlertActionManager actionManager) {
        super(settings);
        this.alertManager = alertManager;
        this.client = client;
        this.scriptService = scriptService;
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

    public void executeAlert(String alertName){
        logger.warn("Running [{}]",alertName);
        Alert alert = alertManager.getAlertForName(alertName);
        //@TODO : claim alert
        String queryTemplate = null;
        try {
            logger.warn("Getting the query");
            GetIndexedScriptResponse scriptResponse = client.prepareGetIndexedScript().setScriptLang("mustache").setId(alert.queryName()).execute().get();
            logger.warn("Got the query");
            if (scriptResponse.isExists()) {
                queryTemplate = scriptResponse.getScript();
            }
            logger.warn("Found : [{}]", queryTemplate);
            if (queryTemplate != null) {
                CompiledScript compiledTemplate = scriptService.compile("mustache",queryTemplate);
                ExecutableScript executable = scriptService.executable(compiledTemplate, new HashMap());
                BytesReference processedQuery = (BytesReference) executable.run();
                logger.warn("Compiled to [{}]", processedQuery);
                SearchResponse sr = client.prepareSearch().setSource(processedQuery).execute().get();
                logger.warn("Got search response");
                AlertResult result = new AlertResult();
                result.isTriggered = triggerManager.isTriggered(alertName,sr);
                result.searchResponse = sr;
                if (result.isTriggered) {
                    logger.warn("We have triggered");
                    actionManager.doAction(alertName,result);
                    logger.warn("Did action !");
                }else{
                    logger.warn("We didn't trigger");
                }
                //@TODO write this back to the alert manager
            } else {
                logger.error("Failed to find query named [{}]",alert.queryName());
            }
        } catch (Exception e) {
            logger.error("Fail", e);
            logger.error("Failed execute alert [{}]",alert.queryName(), e);
        }

    }

    public void addAlert(String alertName, Alert alert) {
        try {
            org.elasticsearch.alerting.AlertExecutorJob.class.newInstance();
        } catch (Exception e){
            logger.error("NOOOOO",e);
        }
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
