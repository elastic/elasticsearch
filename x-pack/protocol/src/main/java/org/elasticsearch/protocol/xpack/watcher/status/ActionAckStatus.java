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
package org.elasticsearch.protocol.xpack.watcher.status;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.protocol.xpack.watcher.status.WatcherDateTimeUtils.dateTimeFormatter;

public class ActionAckStatus implements ToXContentObject {

    public enum State {
        AWAITS_SUCCESSFUL_EXECUTION((byte) 1),
        ACKABLE((byte) 2),
        ACKED((byte) 3);

        private byte value;

        State(byte value) {
            this.value = value;
        }

        static State resolve(byte value) {
            switch (value) {
                case 1 : return AWAITS_SUCCESSFUL_EXECUTION;
                case 2 : return ACKABLE;
                case 3 : return ACKED;
                default:
                    throw new IllegalArgumentException(format("unknown action ack status state value [{}]", value));
            }
        }
    }

    private final DateTime timestamp;
    private final State state;

    public ActionAckStatus(DateTime timestamp, State state) {
        this.timestamp = timestamp.toDateTime(DateTimeZone.UTC);
        this.state = state;
    }

    public DateTime timestamp() {
        return timestamp;
    }

    public State state() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionAckStatus ackStatus = (ActionAckStatus) o;

        return Objects.equals(timestamp, ackStatus.timestamp) &&  Objects.equals(state, ackStatus.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, state);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field(ActionStatusField.TIMESTAMP.getPreferredName()).value(dateTimeFormatter.printer().print(timestamp))
                .field(ActionStatusField.ACK_STATUS_STATE.getPreferredName(), state.name().toLowerCase(Locale.ROOT))
                .endObject();
    }

    public static ActionAckStatus parse(String watchId, String actionId, XContentParser parser) throws IOException {
        DateTime timestamp = null;
        State state = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ActionStatusField.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                timestamp = dateTimeFormatter.parser().parseDateTime(parser.text());
            } else if (ActionStatusField.ACK_STATUS_STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                state = State.valueOf(parser.text().toUpperCase(Locale.ROOT));
            } else {
                throw new ElasticsearchParseException("could not parse action status for [{}/{}]. unexpected field [{}.{}]", watchId,
                        actionId, ActionStatusField.ACK_STATUS.getPreferredName(), currentFieldName);
            }
        }
        if (timestamp == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId, actionId, ActionStatusField.ACK_STATUS.getPreferredName(), ActionStatusField.TIMESTAMP.getPreferredName());
        }
        if (state == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId, actionId, ActionStatusField.ACK_STATUS.getPreferredName(),
                    ActionStatusField.ACK_STATUS_STATE.getPreferredName());
        }
        return new ActionAckStatus(timestamp, state);
    }

    public static void writeTo(ActionAckStatus status, StreamOutput out) throws IOException {
        out.writeLong(status.timestamp.getMillis());
        out.writeByte(status.state.value);
    }

    public static ActionAckStatus readFrom(StreamInput in) throws IOException {
        DateTime timestamp = new DateTime(in.readLong(), DateTimeZone.UTC);
        State state = State.resolve(in.readByte());
        return new ActionAckStatus(timestamp, state);
    }

    @Override
    public String toString() {
        return "ActionAckStatus{" +
                "timestamp=" + timestamp +
                ", state=" + state +
                '}';
    }
}
