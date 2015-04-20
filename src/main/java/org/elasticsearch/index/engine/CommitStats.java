/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.index.engine;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class CommitStats implements Streamable, ToXContent {

    Map<String, String> userData;
    long generation;
    String id; // lucene commit id in base 64;

    public CommitStats(SegmentInfos segmentInfos) {
        // clone the map to protect against concurrent changes
        userData = MapBuilder.<String, String>newMapBuilder().putAll(segmentInfos.getUserData()).immutableMap();
        // lucene calls the current generation, last generation.
        generation = segmentInfos.getLastGeneration();
        id = segmentInfos.getSegmentsFileName();
    }

    private CommitStats() {

    }

    public static CommitStats readCommitStatsFrom(StreamInput in) throws IOException {
        CommitStats commitStats = new CommitStats();
        commitStats.readFrom(in);
        return commitStats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}
