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
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.watcher.actions.WatcherActionModule;
import org.elasticsearch.watcher.client.WatcherClientModule;
import org.elasticsearch.watcher.condition.ConditionModule;
import org.elasticsearch.watcher.execution.ExecutionModule;
import org.elasticsearch.watcher.history.HistoryModule;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.input.InputModule;
import org.elasticsearch.watcher.input.chain.ChainInputFactory;
import org.elasticsearch.watcher.rest.action.RestAckWatchAction;
import org.elasticsearch.watcher.rest.action.RestActivateWatchAction;
import org.elasticsearch.watcher.rest.action.RestDeleteWatchAction;
import org.elasticsearch.watcher.rest.action.RestExecuteWatchAction;
import org.elasticsearch.watcher.rest.action.RestGetWatchAction;
import org.elasticsearch.watcher.rest.action.RestHijackOperationAction;
import org.elasticsearch.watcher.rest.action.RestPutWatchAction;
import org.elasticsearch.watcher.rest.action.RestWatchServiceAction;
import org.elasticsearch.watcher.rest.action.RestWatcherStatsAction;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry.TemplateConfig;
import org.elasticsearch.watcher.support.clock.ClockModule;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpClientModule;
import org.elasticsearch.watcher.support.ScriptServiceProxy;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.watcher.support.secret.SecretModule;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.elasticsearch.watcher.support.text.TextTemplateModule;
import org.elasticsearch.watcher.support.validation.WatcherSettingsValidation;
import org.elasticsearch.watcher.transform.TransformModule;
import org.elasticsearch.watcher.transform.chain.ChainTransformFactory;
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
import org.elasticsearch.xpack.common.init.LazyInitializationModule;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.InternalEmailService;
import org.elasticsearch.xpack.notification.hipchat.HipChatService;
import org.elasticsearch.xpack.notification.hipchat.InternalHipChatService;
import org.elasticsearch.xpack.notification.pagerduty.InternalPagerDutyService;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyAccount;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyService;
import org.elasticsearch.xpack.notification.slack.InternalSlackService;
import org.elasticsearch.xpack.notification.slack.SlackService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class Watcher {

    public static final String NAME = "watcher";

    public static final Setting<String> INDEX_WATCHER_VERSION_SETTING =
            new Setting<>("index.xpack.watcher.plugin.version", "", Function.identity(), Setting.Property.IndexScope);
    public static final Setting<String> INDEX_WATCHER_TEMPLATE_VERSION_SETTING =
            new Setting<>("index.xpack.watcher.template.version", "", Function.identity(), Setting.Property.IndexScope);

    private static final ESLogger logger = Loggers.getLogger(XPackPlugin.class);

    static {
        MetaData.registerPrototype(WatcherMetaData.TYPE, WatcherMetaData.PROTO);
    }

    protected final Settings settings;
    protected final boolean transportClient;
    protected final boolean enabled;

    public Watcher(Settings settings) {
        this.settings = settings;
        transportClient = "transport".equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
        enabled = enabled(settings);
        validAutoCreateIndex(settings);
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(new WatcherModule(enabled, transportClient));
        if (enabled && transportClient == false) {
            modules.add(new WatchModule());
            modules.add(new TextTemplateModule());
            modules.add(new HttpClientModule());
            modules.add(new ClockModule());
            modules.add(new WatcherClientModule());
            modules.add(new TransformModule());
            modules.add(new TriggerModule(settings));
            modules.add(new ScheduleModule());
            modules.add(new ConditionModule());
            modules.add(new InputModule());
            modules.add(new WatcherActionModule());
            modules.add(new HistoryModule());
            modules.add(new ExecutionModule());
            modules.add(new SecretModule(settings));
        }
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (enabled == false|| transportClient) {
            return Collections.emptyList();
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(
            WatcherLicensee.class,
            EmailService.class,
            HipChatService.class,
            SlackService.class,
            PagerDutyService.class,
            HttpClient.class,
            WatcherSettingsValidation.class);
    }

    public Settings additionalSettings() {
        if (enabled == false || transportClient) {
            return Settings.EMPTY;
        }
        Settings additionalSettings = Settings.builder()
                .put(HistoryModule.additionalSettings(settings))
                .build();

        return additionalSettings;
    }

    public void onModule(ScriptModule module) {
        module.registerScriptContext(ScriptServiceProxy.INSTANCE);
    }

    public void onModule(SettingsModule module) {
        for (TemplateConfig templateConfig : WatcherIndexTemplateRegistry.TEMPLATE_CONFIGS) {
            module.registerSetting(templateConfig.getSetting());
        }
        module.registerSetting(InternalSlackService.SLACK_ACCOUNT_SETTING);
        module.registerSetting(InternalEmailService.EMAIL_ACCOUNT_SETTING);
        module.registerSetting(InternalHipChatService.HIPCHAT_ACCOUNT_SETTING);
        module.registerSetting(InternalPagerDutyService.PAGERDUTY_ACCOUNT_SETTING);
        module.registerSetting(INDEX_WATCHER_VERSION_SETTING);
        module.registerSetting(INDEX_WATCHER_TEMPLATE_VERSION_SETTING);
        module.registerSetting(Setting.intSetting("xpack.watcher.execution.scroll.size", 0, Setting.Property.NodeScope));
        module.registerSetting(Setting.intSetting("xpack.watcher.watch.scroll.size", 0, Setting.Property.NodeScope));
        module.registerSetting(Setting.boolSetting(XPackPlugin.featureEnabledSetting(Watcher.NAME), true, Setting.Property.NodeScope));
        module.registerSetting(SecretService.Secure.ENCRYPT_SENSITIVE_DATA_SETTING);

        module.registerSetting(Setting.simpleString("xpack.watcher.internal.ops.search.default_timeout", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.internal.ops.bulk.default_timeout", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.internal.ops.index.default_timeout", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.execution.default_throttle_period", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.http.default_read_timeout", Setting.Property.NodeScope));
        module.registerSetting(Setting.groupSetting("xpack.watcher.http.ssl.", Setting.Property.NodeScope));
        module.registerSetting(Setting.groupSetting("xpack.watcher.http.proxy.", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.actions.index.default_timeout", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.index.rest.direct_access", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.trigger.schedule.engine", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.input.search.default_timeout", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.transform.search.default_timeout", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.trigger.schedule.ticker.tick_interval", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.execution.scroll.timeout", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.start_immediately", Setting.Property.NodeScope));
        module.registerSetting(Setting.simpleString("xpack.watcher.http.default_connection_timeout", Setting.Property.NodeScope));

        module.registerSettingsFilter("xpack.notification.email.account.*.smtp.password");
        module.registerSettingsFilter("xpack.notification.slack.account.*.url");
        module.registerSettingsFilter("xpack.notification.pagerduty.account.*.url");
        module.registerSettingsFilter("xpack.notification.pagerduty." + PagerDutyAccount.SERVICE_KEY_SETTING);
        module.registerSettingsFilter("xpack.notification.pagerduty.account.*." + PagerDutyAccount.SERVICE_KEY_SETTING);
        module.registerSettingsFilter("xpack.notification.hipchat.account.*.auth_token");
    }

    public void onModule(NetworkModule module) {
        if (enabled && transportClient == false) {
            module.registerRestHandler(RestPutWatchAction.class);
            module.registerRestHandler(RestDeleteWatchAction.class);
            module.registerRestHandler(RestWatcherStatsAction.class);
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

    public void onModule(LazyInitializationModule module) {
        if (enabled) {
            module.registerLazyInitializable(WatcherClientProxy.class);
            module.registerLazyInitializable(ChainTransformFactory.class);
            module.registerLazyInitializable(ChainInputFactory.class);
        }
    }

    public static boolean enabled(Settings settings) {
        return XPackPlugin.featureEnabled(settings, NAME, true);
    }

    static void validAutoCreateIndex(Settings settings) {
        String value = settings.get("action.auto_create_index");
        if (value == null) {
            return;
        }

        String errorMessage = LoggerMessageFormat.format("the [action.auto_create_index] setting value [{}] is too" +
                " restrictive. disable [action.auto_create_index] or set it to " +
                "[.watches,.triggered_watches,.watcher-history*]", (Object) value);
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
        logger.warn("the [action.auto_create_index] setting is configured to be restrictive [{}]. " +
                " for the next 6 months daily history indices are allowed to be created, but please make sure" +
                " that any future history indices after 6 months with the pattern " +
                "[.watcher-history-YYYY.MM.dd] are allowed to be created", value);
    }

}
