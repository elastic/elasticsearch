/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.dump;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.dump.heap.HeapDumpContributor;
import org.elasticsearch.monitor.dump.summary.SummaryDumpContributor;
import org.elasticsearch.monitor.dump.thread.ThreadDumpContributor;

import java.io.File;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.monitor.dump.heap.HeapDumpContributor.HEAP_DUMP;
import static org.elasticsearch.monitor.dump.summary.SummaryDumpContributor.SUMMARY;
import static org.elasticsearch.monitor.dump.thread.ThreadDumpContributor.THREAD_DUMP;

/**
 *
 */
public class DumpMonitorService extends AbstractComponent {

    private final String dumpLocation;

    private final DumpGenerator generator;

    private final ClusterService clusterService;
    private final Map<String, Settings> contSettings;
    private final Map<String, DumpContributorFactory> contributors;
    private final File workFile;

    public DumpMonitorService() {
        this(EMPTY_SETTINGS, new Environment(EMPTY_SETTINGS), null, null);
    }

    @Inject
    public DumpMonitorService(Settings settings, Environment environment,
                              @Nullable ClusterService clusterService, @Nullable Map<String, DumpContributorFactory> contributors) {
        super(settings);
        this.clusterService = clusterService;
        this.contributors = contributors;
        contSettings = settings.getGroups("monitor.dump");
        workFile = environment.workWithClusterFile();

        this.dumpLocation = settings.get("dump_location");

        File dumpLocationFile;
        if (dumpLocation != null) {
            dumpLocationFile = new File(dumpLocation);
        } else {
            dumpLocationFile = new File(workFile, "dump");
        }

        Map<String, DumpContributor> contributorMap = newHashMap();
        if (contributors != null) {
            for (Map.Entry<String, DumpContributorFactory> entry : contributors.entrySet()) {
                String contName = entry.getKey();
                DumpContributorFactory dumpContributorFactory = entry.getValue();

                Settings analyzerSettings = contSettings.get(contName);
                if (analyzerSettings == null) {
                    analyzerSettings = EMPTY_SETTINGS;
                }

                DumpContributor analyzerFactory = dumpContributorFactory.create(contName, analyzerSettings);
                contributorMap.put(contName, analyzerFactory);
            }
        }
        if (!contributorMap.containsKey(SUMMARY)) {
            contributorMap.put(SUMMARY, new SummaryDumpContributor(SUMMARY, EMPTY_SETTINGS));
        }
        if (!contributorMap.containsKey(HEAP_DUMP)) {
            contributorMap.put(HEAP_DUMP, new HeapDumpContributor(HEAP_DUMP, EMPTY_SETTINGS));
        }
        if (!contributorMap.containsKey(THREAD_DUMP)) {
            contributorMap.put(THREAD_DUMP, new ThreadDumpContributor(THREAD_DUMP, EMPTY_SETTINGS));
        }
        generator = new SimpleDumpGenerator(dumpLocationFile, contributorMap);
    }

    public DumpGenerator.Result generateDump(String cause, @Nullable Map<String, Object> context) throws DumpGenerationFailedException {
        return generator.generateDump(cause, fillContextMap(context));
    }

    public DumpGenerator.Result generateDump(String cause, @Nullable Map<String, Object> context, String... contributors) throws DumpGenerationFailedException {
        return generator.generateDump(cause, fillContextMap(context), contributors);
    }

    private Map<String, Object> fillContextMap(Map<String, Object> context) {
        if (context == null) {
            context = newHashMap();
        }
        if (clusterService != null) {
            context.put("localNode", clusterService.localNode());
        }
        return context;
    }
}
