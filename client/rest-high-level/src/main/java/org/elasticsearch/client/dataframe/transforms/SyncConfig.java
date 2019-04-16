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

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SyncConfig implements ToXContentObject {
    private static final ParseField TIME_BASED_SYNC = new ParseField("time");

    private final TimeSyncConfig timeSyncConfig;

    private static final ConstructingObjectParser<SyncConfig, Void> PARSER = new ConstructingObjectParser<>("sync_config", true,
            args -> new SyncConfig((TimeSyncConfig) args[0]));

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> TimeSyncConfig.fromXContent(p), TIME_BASED_SYNC);
    }

    public static SyncConfig fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public SyncConfig(TimeSyncConfig timeSyncConfig) {
        this.timeSyncConfig = timeSyncConfig;
    }

    public TimeSyncConfig getTimeSyncConfig() {
        return timeSyncConfig;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (timeSyncConfig != null) {
            builder.field(TIME_BASED_SYNC.getPreferredName(), timeSyncConfig);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SyncConfig that = (SyncConfig) other;

        return Objects.equals(this.timeSyncConfig, that.timeSyncConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeSyncConfig);
    }

}
