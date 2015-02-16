/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.actions.email.service.Authentication;
import org.elasticsearch.alerts.actions.email.service.Email;
import org.elasticsearch.alerts.actions.email.service.EmailService;
import org.elasticsearch.alerts.actions.email.service.Profile;
import org.elasticsearch.alerts.scheduler.schedule.CronSchedule;
import org.elasticsearch.alerts.support.StringTemplateUtils;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import javax.mail.MessagingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
*/
public class EmailActionTest extends ElasticsearchTestCase {

    public void testEmailTemplateRender() throws IOException, MessagingException {
        //createIndex("my-trigger-index");
        StringTemplateUtils.Template template =
                new StringTemplateUtils.Template("{{alert_name}} triggered with {{response.hits.total}} hits");

        Settings settings = ImmutableSettings.settingsBuilder().build();
        MustacheScriptEngineService mustacheScriptEngineService = new MustacheScriptEngineService(settings);
        ThreadPool threadPool = new ThreadPool(ThreadPool.Names.SAME);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(mustacheScriptEngineService);

        ScriptService scriptService = new ScriptService(settings, new Environment(), engineServiceSet, new ResourceWatcherService(settings, threadPool));
        StringTemplateUtils stringTemplateUtils = new StringTemplateUtils(settings, ScriptServiceProxy.of(scriptService));

        EmailService emailService = new EmailServiceMock();

        Email.Address from = new Email.Address("from@test.com");
        List<Email.Address> emailAddressList = new ArrayList<>();
        emailAddressList.add(new Email.Address("to@test.com"));
        Email.AddressList to = new Email.AddressList(emailAddressList);


        Email.Builder emailBuilder = Email.builder();
        emailBuilder.from(from);
        emailBuilder.to(to);

        EmailAction emailAction = new EmailAction(logger, emailService, stringTemplateUtils, emailBuilder,
                new Authentication("testname", "testpassword"), Profile.STANDARD, "testaccount", template, template, null, true);

        //This is ok since the execution of the action only relies on the alert name
        Alert alert = new Alert(
                "test-serialization",
                new CronSchedule("0/5 * * * * ? *"),
                null,
                null,
                new TimeValue(0),
                null,
                null,
                new Alert.Status()
        );
        ExecutionContext ctx = new ExecutionContext("test-serialization#1", alert, new DateTime(), new DateTime());
        EmailAction.Result result = emailAction.execute(ctx, new Payload.Simple());

        threadPool.shutdownNow();
        
        assertTrue(result.success());

        EmailAction.Result.Success success = (EmailAction.Result.Success) result;
        assertEquals(success.account(), "testaccount");
        assertArrayEquals(success.email().to().toArray(), to.toArray() );
        assertEquals(success.email().from(), from);
        //@TODO add more here
    }

    static class EmailServiceMock implements EmailService {
        @Override
        public void start(ClusterState state) {

        }

        @Override
        public void stop() {

        }

        @Override
        public EmailSent send(Email email, Authentication auth, Profile profile) {
            return new EmailSent(auth.username(), email);
        }

        @Override
        public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) {
            return new EmailSent(accountName, email);
        }
    }

}
