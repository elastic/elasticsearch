/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.ingest.DropProcessor;
import org.elasticsearch.ingest.PipelineProcessor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Map.entry;

public class IngestCommonPlugin extends Plugin implements ActionPlugin, IngestPlugin {

    public IngestCommonPlugin() {}

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.ofEntries(
            entry(AppendProcessor.TYPE, new AppendProcessor.Factory(parameters.scriptService)),
            entry(BytesProcessor.TYPE, new BytesProcessor.Factory()),
            entry(CommunityIdProcessor.TYPE, new CommunityIdProcessor.Factory()),
            entry(ConvertProcessor.TYPE, new ConvertProcessor.Factory()),
            entry(CsvProcessor.TYPE, new CsvProcessor.Factory()),
            entry(DateIndexNameProcessor.TYPE, new DateIndexNameProcessor.Factory(parameters.scriptService)),
            entry(DateProcessor.TYPE, new DateProcessor.Factory(parameters.scriptService)),
            entry(DissectProcessor.TYPE, new DissectProcessor.Factory()),
            entry(DotExpanderProcessor.TYPE, new DotExpanderProcessor.Factory()),
            entry(DropProcessor.TYPE, new DropProcessor.Factory()),
            entry(FailProcessor.TYPE, new FailProcessor.Factory(parameters.scriptService)),
            entry(FingerprintProcessor.TYPE, new FingerprintProcessor.Factory()),
            entry(ForEachProcessor.TYPE, new ForEachProcessor.Factory(parameters.scriptService)),
            entry(GrokProcessor.TYPE, new GrokProcessor.Factory(parameters.matcherWatchdog)),
            entry(GsubProcessor.TYPE, new GsubProcessor.Factory()),
            entry(HtmlStripProcessor.TYPE, new HtmlStripProcessor.Factory()),
            entry(JoinProcessor.TYPE, new JoinProcessor.Factory()),
            entry(JsonProcessor.TYPE, new JsonProcessor.Factory()),
            entry(KeyValueProcessor.TYPE, new KeyValueProcessor.Factory(parameters.scriptService)),
            entry(LowercaseProcessor.TYPE, new LowercaseProcessor.Factory()),
            entry(NetworkDirectionProcessor.TYPE, new NetworkDirectionProcessor.Factory(parameters.scriptService)),
            entry(PipelineProcessor.TYPE, new PipelineProcessor.Factory(parameters.ingestService)),
            entry(RegisteredDomainProcessor.TYPE, new RegisteredDomainProcessor.Factory()),
            entry(RemoveProcessor.TYPE, new RemoveProcessor.Factory(parameters.scriptService)),
            entry(RenameProcessor.TYPE, new RenameProcessor.Factory(parameters.scriptService)),
            entry(RerouteProcessor.TYPE, new RerouteProcessor.Factory()),
            entry(ScriptProcessor.TYPE, new ScriptProcessor.Factory(parameters.scriptService)),
            entry(SetProcessor.TYPE, new SetProcessor.Factory(parameters.scriptService)),
            entry(SortProcessor.TYPE, new SortProcessor.Factory()),
            entry(SplitProcessor.TYPE, new SplitProcessor.Factory()),
            entry(TerminateProcessor.TYPE, new TerminateProcessor.Factory()),
            entry(TrimProcessor.TYPE, new TrimProcessor.Factory()),
            entry(URLDecodeProcessor.TYPE, new URLDecodeProcessor.Factory()),
            entry(UppercaseProcessor.TYPE, new UppercaseProcessor.Factory()),
            entry(UriPartsProcessor.TYPE, new UriPartsProcessor.Factory())
        );
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(new ActionHandler(GrokProcessorGetAction.INSTANCE, GrokProcessorGetAction.TransportAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new GrokProcessorGetAction.RestAction());
    }

}
