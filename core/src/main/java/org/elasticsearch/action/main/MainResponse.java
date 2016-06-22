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

package org.elasticsearch.action.main;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class MainResponse extends ActionResponse implements ToXContent {

    private String nodeName;
    private Version version;
    private ClusterName clusterName;
    private Build build;
    private boolean available;

    MainResponse() {
    }

    public MainResponse(String nodeName, Version version, ClusterName clusterName, Build build, boolean available) {
        this.nodeName = nodeName;
        this.version = version;
        this.clusterName = clusterName;
        this.build = build;
        this.available = available;
    }

    public String getNodeName() {
        return nodeName;
    }

    public Version getVersion() {
        return version;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public Build getBuild() {
        return build;
    }

    public boolean isAvailable() {
        return available;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeName);
        Version.writeVersion(version, out);
        clusterName.writeTo(out);
        Build.writeBuild(build, out);
        out.writeBoolean(available);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodeName = in.readString();
        version = Version.readVersion(in);
        clusterName = new ClusterName(in);
        build = Build.readBuild(in);
        available = in.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", nodeName);
        builder.field("cluster_name", clusterName.value());
        builder.startObject("version")
            .field("number", version.toString())
            .field("build_hash", build.shortHash())
            .field("build_date", build.date())
            .field("build_snapshot", build.isSnapshot())
            .field("lucene_version", version.luceneVersion.toString())
            .endObject();
        builder.field("tagline", "You Know, for Search");
        builder.endObject();
        return builder;
    }
}
