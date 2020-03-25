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

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TimeSyncConfig implements SyncConfig {

    public static final String NAME = "time";

    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField DELAY = new ParseField("delay");

    private final String field;
    private final TimeValue delay;

    private static final ConstructingObjectParser<TimeSyncConfig, Void> PARSER = new ConstructingObjectParser<>("time_sync_config", true,
            args -> new TimeSyncConfig((String) args[0], args[1] != null ? (TimeValue) args[1] : TimeValue.ZERO));

    static {
        PARSER.declareString(constructorArg(), FIELD);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DELAY.getPreferredName()), DELAY,
                ObjectParser.ValueType.STRING_OR_NULL);
    }

    public static TimeSyncConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public TimeSyncConfig(String field, TimeValue delay) {
        this.field = field;
        this.delay = delay;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        if (delay.duration() > 0) {
            builder.field(DELAY.getPreferredName(), delay.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    public String getField() {
        return field;
    }

    public TimeValue getDelay() {
        return delay;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final TimeSyncConfig that = (TimeSyncConfig) other;

        return Objects.equals(this.field, that.field)
                && Objects.equals(this.delay, that.delay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, delay);
    }

    @Override
    public String getName() {
        return NAME;
    }

}
