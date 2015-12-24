/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.watcher.actions.WatcherActionModule;
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.actions.email.service.InternalEmailService;
import org.elasticsearch.watcher.actions.hipchat.service.HipChatService;
import org.elasticsearch.watcher.actions.hipchat.service.InternalHipChatService;
import org.elasticsearch.watcher.actions.pagerduty.service.InternalPagerDutyService;
import org.elasticsearch.watcher.actions.pagerduty.service.PagerDutyService;
import org.elasticsearch.watcher.actions.slack.service.InternalSlackService;
import org.elasticsearch.watcher.actions.slack.service.SlackService;
import org.elasticsearch.watcher.client.WatcherClientModule;
import org.elasticsearch.watcher.condition.ConditionModule;
import org.elasticsearch.watcher.execution.ExecutionModule;
import org.elasticsearch.watcher.history.HistoryModule;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.input.InputModule;
import org.elasticsearch.watcher.license.LicenseModule;
import org.elasticsearch.watcher.license.WatcherLicensee;
import org.elasticsearch.watcher.rest.action.RestAckWatchAction;
import org.elasticsearch.watcher.rest.action.RestActivateWatchAction;
import org.elasticsearch.watcher.rest.action.RestDeleteWatchAction;
import org.elasticsearch.watcher.rest.action.RestExecuteWatchAction;
import org.elasticsearch.watcher.rest.action.RestGetWatchAction;
import org.elasticsearch.watcher.rest.action.RestHijackOperationAction;
import org.elasticsearch.watcher.rest.action.RestPutWatchAction;
import org.elasticsearch.watcher.rest.action.RestWatchServiceAction;
import org.elasticsearch.watcher.rest.action.RestWatcherInfoAction;
import org.elasticsearch.watcher.rest.action.RestWatcherStatsAction;
import org.elasticsearch.watcher.shield.WatcherShieldModule;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry.TemplateConfig;
import org.elasticsearch.watcher.support.clock.ClockModule;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpClientModule;
import org.elasticsearch.watcher.support.init.InitializingModule;
import org.elasticsearch.watcher.support.init.InitializingService;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.support.secret.SecretModule;
import org.elasticsearch.watcher.support.text.TextTemplateModule;
import org.elasticsearch.watcher.support.validation.WatcherSettingsValidation;
import org.elasticsearch.watcher.transform.TransformModule;
import org.elasticsearch.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.watcher.transport.actions.ack.TransportAckWatchAction;
import org.elasticsearch.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.watcher.transport.actions.activate.TransportActivateWatchAction;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.watcher.transport.actions.delete.TransportDeleteWatchAction;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.watcher.transport.actions.execute.TransportExecuteWatchAction;
import org.elasticsearch.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.watcher.transport.actions.get.TransportGetWatchAction;
import org.elasticsearch.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.watcher.transport.actions.put.TransportPutWatchAction;
import org.elasticsearch.watcher.transport.actions.service.TransportWatcherServiceAction;
import org.elasticsearch.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.watcher.transport.actions.stats.TransportWatcherStatsAction;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.watcher.trigger.TriggerModule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleModule;
import org.elasticsearch.watcher.watch.WatchModule;
import org.elasticsearch.xpack.XPackPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class WatcherPlugin extends Plugin {

    public static final String NAME = "watcher";
    public static final String ENABLED_SETTING = NAME + ".enabled";

    private final static ESLogger logger = Loggers.getLogger(XPackPlugin.class);

    static {
        MetaData.registerPrototype(WatcherMetaData.TYPE, WatcherMetaData.PROTO);
    }

    protected final Settings settings;
    protected final boolean transportClient;
    protected final boolean enabled;

    public WatcherPlugin(Settings settings) {
        this.settings = settings;
        transportClient = "transport".equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
        enabled = watcherEnabled(settings);
        validAutoCreateIndex(settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public String description() {
        return "Elasticsearch Watcher";
    }

    @Override
    public Collection<Module> nodeModules() {
        if (!enabled || transportClient) {
            return Collections.emptyList();
        }
        return Arrays.<Module>asList(
                new WatcherModule(settings),
                new InitializingModule(),
                new LicenseModule(),
                new WatchModule(),
                new TextTemplateModule(),
                new HttpClientModule(),
                new ClockModule(),
                new WatcherClientModule(),
                new TransformModule(),
                new TriggerModule(settings),
                new ScheduleModule(),
                new ConditionModule(),
                new InputModule(),
                new WatcherActionModule(),
                new HistoryModule(),
                new ExecutionModule(),
                new WatcherShieldModule(settings),
                new SecretModule(settings));
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (!enabled || transportClient) {
            return Collections.emptyList();
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(
            // the initialization service must be first in the list
            // as other services may depend on one of the initialized
            // constructs
            InitializingService.class,
            WatcherLicensee.class,
            EmailService.class,
            HipChatService.class,
            SlackService.class,
            PagerDutyService.class,
            HttpClient.class,
            WatcherSettingsValidation.class);
    }

    @Override
    public Settings additionalSettings() {
        if (!enabled || transportClient) {
            return Settings.EMPTY;
        }
        Settings additionalSettings = settingsBuilder()
                .put(HistoryModule.additionalSettings(settings))
                .build();

        return additionalSettings;
    }

    public void onModule(ScriptModule module) {
        module.registerScriptContext(ScriptServiceProxy.INSTANCE);
    }

    public void onModule(SettingsModule module) {
        for (TemplateConfig templateConfig : WatcherModule.TEMPLATE_CONFIGS) {
            module.registerSetting(templateConfig.getSetting());
        }
        module.registerSetting(InternalSlackService.SLACK_ACCOUNT_SETTING);
        module.registerSetting(InternalEmailService.EMAIL_ACCOUNT_SETTING);
        module.registerSetting(InternalHipChatService.HIPCHAT_ACCOUNT_SETTING);
        module.registerSetting(InternalPagerDutyService.PAGERDUTY_ACCOUNT_SETTING);
    }

    public void onModule(NetworkModule module) {
        if (enabled && !transportClient) {
            module.registerRestHandler(RestPutWatchAction.class);
            module.registerRestHandler(RestDeleteWatchAction.class);
            module.registerRestHandler(RestWatcherStatsAction.class);
            module.registerRestHandler(RestWatcherInfoAction.class);
            module.registerRestHandler(RestGetWatchAction.class);
            module.registerRestHandler(RestWatchServiceAction.class);
            module.registerRestHandler(RestAckWatchAction.class);
            module.registerRestHandler(RestActivateWatchAction.class);
            module.registerRestHandler(RestExecuteWatchAction.class);
            module.registerRestHandler(RestHijackOperationAction.class);
        }
    }

    public void onModule(ActionModule module) {
        if (enabled) {
            module.registerAction(PutWatchAction.INSTANCE, TransportPutWatchAction.class);
            module.registerAction(DeleteWatchAction.INSTANCE, TransportDeleteWatchAction.class);
            module.registerAction(GetWatchAction.INSTANCE, TransportGetWatchAction.class);
            module.registerAction(WatcherStatsAction.INSTANCE, TransportWatcherStatsAction.class);
            module.registerAction(AckWatchAction.INSTANCE, TransportAckWatchAction.class);
            module.registerAction(ActivateWatchAction.INSTANCE, TransportActivateWatchAction.class);
            module.registerAction(WatcherServiceAction.INSTANCE, TransportWatcherServiceAction.class);
            module.registerAction(ExecuteWatchAction.INSTANCE, TransportExecuteWatchAction.class);
        }
    }

    public static boolean watcherEnabled(Settings settings) {
        return settings.getAsBoolean(ENABLED_SETTING, true);
    }

    static void validAutoCreateIndex(Settings settings) {
        String value = settings.get("action.auto_create_index");
        if (value == null) {
            return;
        }

        String errorMessage = LoggerMessageFormat.format("the [action.auto_create_index] setting value [{}] is too restrictive. disable [action.auto_create_index] or set it to [.watches,.triggered_watches,.watch_history*]", (Object) settings);
        if (Booleans.isExplicitFalse(value)) {
            throw new IllegalArgumentException(errorMessage);
        }

        if (Booleans.isExplicitTrue(value)) {
            return;
        }

        String[] matches = Strings.commaDelimitedListToStringArray(value);
        List<String> indices = new ArrayList<>();
        indices.add(".watches");
        indices.add(".triggered_watches");
        DateTime now = new DateTime(DateTimeZone.UTC);
        indices.add(HistoryStore.getHistoryIndexNameForTime(now));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusDays(1)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(1)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(2)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(3)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(4)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(5)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(6)));
        for (String index : indices) {
            boolean matched = false;
            for (String match : matches) {
                char c = match.charAt(0);
                if (c == '-') {
                    if (Regex.simpleMatch(match.substring(1), index)) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                } else if (c == '+') {
                    if (Regex.simpleMatch(match.substring(1), index)) {
                        matched = true;
                        break;
                    }
                } else {
                    if (Regex.simpleMatch(match, index)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (!matched) {
                throw new IllegalArgumentException(errorMessage);
            }
        }
        logger.warn("the [action.auto_create_index] setting is configured to be restrictive [{}]. for the next 6 months daily history indices are allowed to be created, but please make sure that any future history indices after 6 months with the pattern [.watch_history-YYYY.MM.dd] are allowed to be created", value);
    }

}
