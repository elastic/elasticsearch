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

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response for a cluster update settings action.
 */
public class ClusterUpdateSettingsResponse extends AcknowledgedResponse {

    private static final ParseField PERSISTENT = new ParseField("persistent");
    private static final ParseField TRANSIENT = new ParseField("transient");

    private static final ConstructingObjectParser<ClusterUpdateSettingsResponse, Void> PARSER = new ConstructingObjectParser<>(
            "cluster_update_settings_response", true, args -> {
                return new ClusterUpdateSettingsResponse((boolean) args[0], (Settings) args[1], (Settings) args[2]);
            });
    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareObject(constructorArg(), (p, c) -> Settings.fromXContent(p), TRANSIENT);
        PARSER.declareObject(constructorArg(), (p, c) -> Settings.fromXContent(p), PERSISTENT);
    }

    final Settings transientSettings;
    final Settings persistentSettings;

    ClusterUpdateSettingsResponse(StreamInput in) throws IOException {
        super(in);
        transientSettings = Settings.readSettingsFromStream(in);
        persistentSettings = Settings.readSettingsFromStream(in);
    }

    ClusterUpdateSettingsResponse(boolean acknowledged, Settings transientSettings, Settings persistentSettings) {
        super(acknowledged);
        this.persistentSettings = persistentSettings;
        this.transientSettings = transientSettings;
    }

    public Settings getTransientSettings() {
        return transientSettings;
    }

    public Settings getPersistentSettings() {
        return persistentSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Settings.writeSettingsToStream(transientSettings, out);
        Settings.writeSettingsToStream(persistentSettings, out);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PERSISTENT.getPreferredName());
        persistentSettings.toXContent(builder, params);
        builder.endObject();
        builder.startObject(TRANSIENT.getPreferredName());
        transientSettings.toXContent(builder, params);
        builder.endObject();
    }

    public static ClusterUpdateSettingsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            ClusterUpdateSettingsResponse that = (ClusterUpdateSettingsResponse) o;
            return Objects.equals(transientSettings, that.transientSettings) &&
                    Objects.equals(persistentSettings, that.persistentSettings);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), transientSettings, persistentSettings);
    }
}
