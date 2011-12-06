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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.FileSystemUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 */
public class SimpleDumpGenerator implements DumpGenerator {

    private final File dumpLocation;

    private final ImmutableMap<String, DumpContributor> contributors;

    public SimpleDumpGenerator(File dumpLocation, Map<String, DumpContributor> contributors) {
        this.dumpLocation = dumpLocation;
        this.contributors = ImmutableMap.copyOf(contributors);
    }

    public Result generateDump(String cause, @Nullable Map<String, Object> context) throws DumpGenerationFailedException {
        return generateDump(cause, context, contributors.keySet().toArray(new String[contributors.size()]));
    }

    public Result generateDump(String cause, @Nullable Map<String, Object> context, String... contributors) throws DumpGenerationFailedException {
        long timestamp = System.currentTimeMillis();
        String fileName = "";
        if (context.containsKey("localNode")) {
            DiscoveryNode localNode = (DiscoveryNode) context.get("localNode");
            if (localNode.name() != null) {
                fileName += localNode.name() + "-";
            }
            fileName += localNode.id() + "-";
        }
        File file = new File(dumpLocation, fileName + cause + "-" + timestamp);
        FileSystemUtils.mkdirs(file);
        SimpleDump dump;
        try {
            dump = new SimpleDump(System.currentTimeMillis(), cause, context, file);
        } catch (FileNotFoundException e) {
            throw new DumpGenerationFailedException("Failed to generate dump", e);
        }
        ArrayList<DumpContributionFailedException> failedContributors = new ArrayList<DumpContributionFailedException>();
        for (String name : contributors) {
            DumpContributor contributor = this.contributors.get(name);
            if (contributor == null) {
                failedContributors.add(new DumpContributionFailedException(name, "No contributor"));
                continue;
            }
            try {
                contributor.contribute(dump);
            } catch (DumpContributionFailedException e) {
                failedContributors.add(e);
            } catch (Exception e) {
                failedContributors.add(new DumpContributionFailedException(contributor.getName(), "Failed", e));
            }
        }
        dump.finish();
        return new Result(file, failedContributors);
    }
}
