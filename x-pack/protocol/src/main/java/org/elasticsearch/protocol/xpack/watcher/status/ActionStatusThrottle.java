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
import java.util.Objects;

import static org.elasticsearch.protocol.xpack.watcher.status.WatcherDateTimeUtils.dateTimeFormatter;

public class ActionStatusThrottle implements ToXContentObject {

    private final DateTime timestamp;
    private final String reason;

    public ActionStatusThrottle(DateTime timestamp, String reason) {
        this.timestamp = timestamp.toDateTime(DateTimeZone.UTC);
        this.reason = reason;
    }

    public DateTime timestamp() {
        return timestamp;
    }

    public String reason() {
        return reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionStatusThrottle throttle = (ActionStatusThrottle) o;
        return Objects.equals(timestamp, throttle.timestamp) && Objects.equals(reason, throttle.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, reason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field(ActionStatusField.TIMESTAMP.getPreferredName()).value(dateTimeFormatter.printer().print(timestamp))
                .field(ActionStatusField.REASON.getPreferredName(), reason)
                .endObject();
    }

    public static ActionStatusThrottle parse(String watchId, String actionId, XContentParser parser) throws IOException {
        DateTime timestamp = null;
        String reason = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ActionStatusField.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                timestamp = dateTimeFormatter.parser().parseDateTime(parser.text());
            } else if (ActionStatusField.REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                reason = parser.text();
            } else {
                throw new ElasticsearchParseException("could not parse action status for [{}/{}]. unexpected field [{}.{}]", watchId,
                        actionId, ActionStatusField.LAST_THROTTLE.getPreferredName(), currentFieldName);
            }
        }
        if (timestamp == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId, actionId, ActionStatusField.LAST_THROTTLE.getPreferredName(), ActionStatusField.TIMESTAMP.getPreferredName());
        }
        if (reason == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId, actionId, ActionStatusField.LAST_THROTTLE.getPreferredName(), ActionStatusField.REASON.getPreferredName());
        }
        return new ActionStatusThrottle(timestamp, reason);
    }

    public static void writeTo(ActionStatusThrottle throttle, StreamOutput out) throws IOException {
        out.writeLong(throttle.timestamp.getMillis());
        out.writeString(throttle.reason);
    }

    public static ActionStatusThrottle readFrom(StreamInput in) throws IOException {
        DateTime timestamp = new DateTime(in.readLong(), DateTimeZone.UTC);
        return new ActionStatusThrottle(timestamp, in.readString());
    }

    @Override
    public String toString() {
        return "ActionStatusThrottle{" +
                "timestamp=" + timestamp +
                ", reason='" + reason + '\'' +
                '}';
    }
}
