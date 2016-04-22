/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.support.ScriptServiceProxy;
import org.junit.Ignore;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Ignore // not a test.
@SuppressForbidden(reason = "gradle is broken and tries to run me as a test")
public final class MessyTestUtils {
    public static ScriptServiceProxy getScriptServiceProxy(ThreadPool tp) throws Exception {
        Settings settings = Settings.builder()
                .put("script.inline", "true")
                .put("script.indexed", "true")
                .put("path.home", LuceneTestCase.createTempDir())
                .build();
        GroovyScriptEngineService groovyScriptEngineService = new GroovyScriptEngineService(settings);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(groovyScriptEngineService);
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(
                Arrays.asList(
                        new ScriptEngineRegistry.ScriptEngineRegistration(GroovyScriptEngineService.class, GroovyScriptEngineService.TYPES)
                )
        );
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Arrays.asList(ScriptServiceProxy.INSTANCE));

        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name")).build());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        return  ScriptServiceProxy.of(new ScriptService(settings, new Environment(settings), engineServiceSet,
                new ResourceWatcherService(settings, tp), scriptEngineRegistry, scriptContextRegistry, scriptSettings),
                clusterService);
    }
}
