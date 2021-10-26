/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.ingest.DropProcessor;
import org.elasticsearch.ingest.PipelineProcessor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Map.entry;

public class IngestCommonPlugin extends Plugin implements ActionPlugin, IngestPlugin {

    static final Setting<TimeValue> WATCHDOG_INTERVAL =
        Setting.timeSetting("ingest.grok.watchdog.interval", TimeValue.timeValueSeconds(1), Setting.Property.NodeScope);
    static final Setting<TimeValue> WATCHDOG_MAX_EXECUTION_TIME =
        Setting.timeSetting("ingest.grok.watchdog.max_execution_time", TimeValue.timeValueSeconds(1), Setting.Property.NodeScope);

    public IngestCommonPlugin() {
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.ofEntries(
                entry(DateProcessor.TYPE, new DateProcessor.Factory(parameters.scriptService)),
                entry(SetProcessor.TYPE, new SetProcessor.Factory(parameters.scriptService)),
                entry(AppendProcessor.TYPE, new AppendProcessor.Factory(parameters.scriptService)),
                entry(RenameProcessor.TYPE, new RenameProcessor.Factory(parameters.scriptService)),
                entry(RemoveProcessor.TYPE, new RemoveProcessor.Factory(parameters.scriptService)),
                entry(SplitProcessor.TYPE, new SplitProcessor.Factory()),
                entry(JoinProcessor.TYPE, new JoinProcessor.Factory()),
                entry(UppercaseProcessor.TYPE, new UppercaseProcessor.Factory()),
                entry(LowercaseProcessor.TYPE, new LowercaseProcessor.Factory()),
                entry(TrimProcessor.TYPE, new TrimProcessor.Factory()),
                entry(ConvertProcessor.TYPE, new ConvertProcessor.Factory()),
                entry(GsubProcessor.TYPE, new GsubProcessor.Factory()),
                entry(FailProcessor.TYPE, new FailProcessor.Factory(parameters.scriptService)),
                entry(ForEachProcessor.TYPE, new ForEachProcessor.Factory(parameters.scriptService)),
                entry(DateIndexNameProcessor.TYPE, new DateIndexNameProcessor.Factory(parameters.scriptService)),
                entry(SortProcessor.TYPE, new SortProcessor.Factory()),
                entry(GrokProcessor.TYPE, new GrokProcessor.Factory(createGrokThreadWatchdog(parameters))),
                entry(ScriptProcessor.TYPE, new ScriptProcessor.Factory(parameters.scriptService)),
                entry(DotExpanderProcessor.TYPE, new DotExpanderProcessor.Factory()),
                entry(JsonProcessor.TYPE, new JsonProcessor.Factory()),
                entry(KeyValueProcessor.TYPE, new KeyValueProcessor.Factory(parameters.scriptService)),
                entry(URLDecodeProcessor.TYPE, new URLDecodeProcessor.Factory()),
                entry(BytesProcessor.TYPE, new BytesProcessor.Factory()),
                entry(PipelineProcessor.TYPE, new PipelineProcessor.Factory(parameters.ingestService)),
                entry(DissectProcessor.TYPE, new DissectProcessor.Factory()),
                entry(DropProcessor.TYPE, new DropProcessor.Factory()),
                entry(HtmlStripProcessor.TYPE, new HtmlStripProcessor.Factory()),
                entry(CsvProcessor.TYPE, new CsvProcessor.Factory()),
                entry(UriPartsProcessor.TYPE, new UriPartsProcessor.Factory()),
                entry(NetworkDirectionProcessor.TYPE, new NetworkDirectionProcessor.Factory(parameters.scriptService)),
                entry(CommunityIdProcessor.TYPE, new CommunityIdProcessor.Factory()),
                entry(FingerprintProcessor.TYPE, new FingerprintProcessor.Factory()),
                entry(RegisteredDomainProcessor.TYPE, new RegisteredDomainProcessor.Factory())
            );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(
                new ActionHandler<>(GrokProcessorGetAction.INSTANCE, GrokProcessorGetAction.TransportAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Collections.singletonList(new GrokProcessorGetAction.RestAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(WATCHDOG_INTERVAL, WATCHDOG_MAX_EXECUTION_TIME);
    }

    private static MatcherWatchdog createGrokThreadWatchdog(Processor.Parameters parameters) {
        long intervalMillis = WATCHDOG_INTERVAL.get(parameters.env.settings()).getMillis();
        long maxExecutionTimeMillis = WATCHDOG_MAX_EXECUTION_TIME.get(parameters.env.settings()).getMillis();
        return MatcherWatchdog.newInstance(intervalMillis, maxExecutionTimeMillis,
            parameters.relativeTimeSupplier, parameters.scheduler::apply);
    }

}
