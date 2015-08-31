/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.shield.authz.AuthorizationModule;
import org.elasticsearch.watcher.actions.WatcherActionModule;
import org.elasticsearch.watcher.actions.email.service.InternalEmailService;
import org.elasticsearch.watcher.actions.hipchat.service.InternalHipChatService;
import org.elasticsearch.watcher.client.WatcherClientModule;
import org.elasticsearch.watcher.condition.ConditionModule;
import org.elasticsearch.watcher.execution.ExecutionModule;
import org.elasticsearch.watcher.history.HistoryModule;
import org.elasticsearch.watcher.input.InputModule;
import org.elasticsearch.watcher.license.LicenseModule;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.rest.action.*;
import org.elasticsearch.watcher.shield.ShieldIntegration;
import org.elasticsearch.watcher.shield.WatcherShieldModule;
import org.elasticsearch.watcher.shield.WatcherUserHolder;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry.TemplateConfig;
import org.elasticsearch.watcher.support.clock.ClockModule;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpClientModule;
import org.elasticsearch.watcher.support.init.InitializingModule;
import org.elasticsearch.watcher.support.init.InitializingService;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.support.secret.SecretModule;
import org.elasticsearch.watcher.support.text.TextTemplateModule;
import org.elasticsearch.watcher.support.text.xmustache.XMustacheScriptEngineService;
import org.elasticsearch.watcher.support.validation.WatcherSettingsValidation;
import org.elasticsearch.watcher.transform.TransformModule;
import org.elasticsearch.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.watcher.transport.actions.ack.TransportAckWatchAction;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class WatcherPlugin extends Plugin {

    public static final String NAME = "watcher";
    public static final String ENABLED_SETTING = NAME + ".enabled";

    static {
        MetaData.registerPrototype(WatcherMetaData.TYPE, WatcherMetaData.PROTO);
    }

    protected final Settings settings;
    protected final boolean transportClient;
    protected final boolean enabled;

    public WatcherPlugin(Settings settings) {
        this.settings = settings;
        transportClient = "transport".equals(settings.get(Client.CLIENT_TYPE_SETTING));
        enabled = watcherEnabled(settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public String description() {
        return "Elasticsearch Watcher";
    }

    @Override
    public Collection<Module> nodeModules() {
        if (enabled == false) {
            return Collections.emptyList();
        } else if (transportClient == false){
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
        return Collections.emptyList();
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (!enabled || transportClient) {
            return Collections.EMPTY_SET;
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(
            // the initialization service must be first in the list
            // as other services may depend on one of the initialized
            // constructs
            InitializingService.class,
            LicenseService.class,
            InternalEmailService.class,
            InternalHipChatService.class,
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
        if (enabled && transportClient == false) {
            module.addScriptEngine(XMustacheScriptEngineService.class);
        }
    }

    public void onModule(ClusterModule module) {
        for (TemplateConfig templateConfig : WatcherModule.TEMPLATE_CONFIGS) {
            module.registerClusterDynamicSetting(templateConfig.getDynamicSettingsPrefix(), Validator.EMPTY);
        }
    }

    public void onModule(RestModule module) {
        if (enabled && transportClient == false) {
            module.addRestAction(RestPutWatchAction.class);
            module.addRestAction(RestDeleteWatchAction.class);
            module.addRestAction(RestWatcherStatsAction.class);
            module.addRestAction(RestWatcherInfoAction.class);
            module.addRestAction(RestGetWatchAction.class);
            module.addRestAction(RestWatchServiceAction.class);
            module.addRestAction(RestAckWatchAction.class);
            module.addRestAction(RestExecuteWatchAction.class);
            module.addRestAction(RestHijackOperationAction.class);
        }
    }

    public void onModule(ActionModule module) {
        if (enabled) {
            module.registerAction(PutWatchAction.INSTANCE, TransportPutWatchAction.class);
            module.registerAction(DeleteWatchAction.INSTANCE, TransportDeleteWatchAction.class);
            module.registerAction(GetWatchAction.INSTANCE, TransportGetWatchAction.class);
            module.registerAction(WatcherStatsAction.INSTANCE, TransportWatcherStatsAction.class);
            module.registerAction(AckWatchAction.INSTANCE, TransportAckWatchAction.class);
            module.registerAction(WatcherServiceAction.INSTANCE, TransportWatcherServiceAction.class);
            module.registerAction(ExecuteWatchAction.INSTANCE, TransportExecuteWatchAction.class);
        }
    }

    // NOTE: The fact this signature takes a module is a hack, and effectively like the previous
    // processModule in the plugin api. The problem is tight coupling between watcher and shield.
    // We need to avoid trying to load the AuthorizationModule class unless we know shield integration
    // is enabled. This is a temporary solution until inter-plugin-communication can be worked out.
    public void onModule(Module module) {
        if (enabled && ShieldIntegration.enabled(settings) && module instanceof AuthorizationModule) {
            ((AuthorizationModule)module).registerReservedRole(WatcherUserHolder.ROLE);
        }
    }

    public static boolean watcherEnabled(Settings settings) {
        return settings.getAsBoolean(ENABLED_SETTING, true);
    }

}
