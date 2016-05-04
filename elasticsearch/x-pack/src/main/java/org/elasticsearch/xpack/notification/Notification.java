/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.InternalEmailService;
import org.elasticsearch.xpack.notification.hipchat.HipChatService;
import org.elasticsearch.xpack.notification.hipchat.InternalHipChatService;
import org.elasticsearch.xpack.notification.pagerduty.InternalPagerDutyService;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyAccount;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyService;
import org.elasticsearch.xpack.notification.slack.InternalSlackService;
import org.elasticsearch.xpack.notification.slack.SlackService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Notification {

    private final boolean transportClient;

    public Notification(Settings settings) {
        this.transportClient = "transport".equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
    }

    public void onModule(SettingsModule module) {
        module.registerSetting(InternalSlackService.SLACK_ACCOUNT_SETTING);
        module.registerSetting(InternalEmailService.EMAIL_ACCOUNT_SETTING);
        module.registerSetting(InternalHipChatService.HIPCHAT_ACCOUNT_SETTING);
        module.registerSetting(InternalPagerDutyService.PAGERDUTY_ACCOUNT_SETTING);

        module.registerSettingsFilter("xpack.notification.email.account.*.smtp.password");
        module.registerSettingsFilter("xpack.notification.slack.account.*.url");
        module.registerSettingsFilter("xpack.notification.pagerduty.account.*.url");
        module.registerSettingsFilter("xpack.notification.pagerduty." + PagerDutyAccount.SERVICE_KEY_SETTING);
        module.registerSettingsFilter("xpack.notification.pagerduty.account.*." + PagerDutyAccount.SERVICE_KEY_SETTING);
        module.registerSettingsFilter("xpack.notification.hipchat.account.*.auth_token");
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (transportClient) {
            return Collections.emptyList();
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(
                HttpClient.class,
                EmailService.class,
                HipChatService.class,
                SlackService.class,
                PagerDutyService.class
        );
    }

    public Collection<? extends Module> nodeModules() {
        if (transportClient) {
            return Collections.emptyList();
        }
        List<Module> modules = new ArrayList<>();
        modules.add(new NotificationModule());
        return modules;
    }
}
