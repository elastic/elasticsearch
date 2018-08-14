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

public class ActionStatusExecution implements ToXContentObject {

    public static ActionStatusExecution successful(DateTime timestamp) {
        return new ActionStatusExecution(timestamp, true, null);
    }

    public static ActionStatusExecution failure(DateTime timestamp, String reason) {
        return new ActionStatusExecution(timestamp, false, reason);
    }

    private final DateTime timestamp;
    private final boolean successful;
    private final String reason;

    private ActionStatusExecution(DateTime timestamp, boolean successful, String reason) {
        this.timestamp = timestamp.toDateTime(DateTimeZone.UTC);
        this.successful = successful;
        this.reason = reason;
    }

    public DateTime timestamp() {
        return timestamp;
    }

    public boolean successful() {
        return successful;
    }

    public String reason() {
        return reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionStatusExecution execution = (ActionStatusExecution) o;

        return Objects.equals(successful, execution.successful) &&
               Objects.equals(timestamp, execution.timestamp) &&
               Objects.equals(reason, execution.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, successful, reason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ActionStatusField.TIMESTAMP.getPreferredName()).value(dateTimeFormatter.printer().print(timestamp));
        builder.field(ActionStatusField.EXECUTION_SUCCESSFUL.getPreferredName(), successful);
        if (reason != null) {
            builder.field(ActionStatusField.REASON.getPreferredName(), reason);
        }
        return builder.endObject();
    }

    public static ActionStatusExecution parse(String watchId, String actionId, XContentParser parser) throws IOException {
        DateTime timestamp = null;
        Boolean successful = null;
        String reason = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ActionStatusField.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                timestamp = dateTimeFormatter.parser().parseDateTime(parser.text());
            } else if (ActionStatusField.EXECUTION_SUCCESSFUL.match(currentFieldName, parser.getDeprecationHandler())) {
                successful = parser.booleanValue();
            } else if (ActionStatusField.REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                reason = parser.text();
            } else {
                throw new ElasticsearchParseException("could not parse action status for [{}/{}]. unexpected field [{}.{}]", watchId,
                        actionId, ActionStatusField.LAST_EXECUTION.getPreferredName(), currentFieldName);
            }
        }
        if (timestamp == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId, actionId, ActionStatusField.LAST_EXECUTION.getPreferredName(), ActionStatusField.TIMESTAMP.getPreferredName());
        }
        if (successful == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId, actionId, ActionStatusField.LAST_EXECUTION.getPreferredName(),
                    ActionStatusField.EXECUTION_SUCCESSFUL.getPreferredName());
        }
        if (successful) {
            return successful(timestamp);
        }
        if (reason == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}/{}]. missing required field for unsuccessful" +
                    " execution [{}.{}]", watchId, actionId, ActionStatusField.LAST_EXECUTION.getPreferredName(),
                    ActionStatusField.REASON.getPreferredName());
        }
        return failure(timestamp, reason);
    }

    public static void writeTo(ActionStatusExecution execution, StreamOutput out) throws IOException {
        out.writeLong(execution.timestamp.getMillis());
        out.writeBoolean(execution.successful);
        if (!execution.successful) {
            out.writeString(execution.reason);
        }
    }

    public static ActionStatusExecution readFrom(StreamInput in) throws IOException {
        DateTime timestamp = new DateTime(in.readLong(), DateTimeZone.UTC);
        boolean successful = in.readBoolean();
        if (successful) {
            return successful(timestamp);
        }
        return failure(timestamp, in.readString());
    }

    @Override
    public String toString() {
        return "ActionStatusExecution{" +
                "timestamp=" + timestamp +
                ", successful=" + successful +
                ", reason='" + reason + '\'' +
                '}';
    }
}
