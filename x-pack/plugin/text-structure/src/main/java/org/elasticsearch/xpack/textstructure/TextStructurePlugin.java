/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.textstructure.action.FindFieldStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindMessageStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.TestGrokPatternAction;
import org.elasticsearch.xpack.textstructure.rest.RestFindFieldStructureAction;
import org.elasticsearch.xpack.textstructure.rest.RestFindMessageStructureAction;
import org.elasticsearch.xpack.textstructure.rest.RestFindStructureAction;
import org.elasticsearch.xpack.textstructure.rest.RestTestGrokPatternAction;
import org.elasticsearch.xpack.textstructure.transport.TransportFindFieldStructureAction;
import org.elasticsearch.xpack.textstructure.transport.TransportFindMessageStructureAction;
import org.elasticsearch.xpack.textstructure.transport.TransportFindStructureAction;
import org.elasticsearch.xpack.textstructure.transport.TransportTestGrokPatternAction;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This plugin provides APIs for text structure analysis.
 *
 */
public class TextStructurePlugin extends Plugin implements ActionPlugin {

    public static final String BASE_PATH = "/_text_structure/";

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return Arrays.asList(
            new RestFindFieldStructureAction(),
            new RestFindMessageStructureAction(),
            new RestFindStructureAction(),
            new RestTestGrokPatternAction()
        );
    }

    @Override
    public List<ActionHandler> getActions() {
        return Arrays.asList(
            new ActionHandler(FindFieldStructureAction.INSTANCE, TransportFindFieldStructureAction.class),
            new ActionHandler(FindMessageStructureAction.INSTANCE, TransportFindMessageStructureAction.class),
            new ActionHandler(FindStructureAction.INSTANCE, TransportFindStructureAction.class),
            new ActionHandler(TestGrokPatternAction.INSTANCE, TransportTestGrokPatternAction.class)
        );
    }
}
