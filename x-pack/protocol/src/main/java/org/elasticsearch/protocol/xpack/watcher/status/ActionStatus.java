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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Objects;

public class ActionStatus implements ToXContentObject {

    private ActionAckStatus ackStatus;
    @Nullable private ActionStatusExecution lastExecution;
    @Nullable private ActionStatusExecution lastSuccessfulExecution;
    @Nullable private ActionStatusThrottle lastThrottle;

    public ActionStatus(DateTime now) {
        this(new ActionAckStatus(now, ActionAckStatus.State.AWAITS_SUCCESSFUL_EXECUTION), null, null, null);
    }

    public ActionStatus(ActionAckStatus ackStatus, @Nullable ActionStatusExecution lastExecution,
                        @Nullable ActionStatusExecution lastSuccessfulExecution, @Nullable ActionStatusThrottle lastThrottle) {
        this.ackStatus = ackStatus;
        this.lastExecution = lastExecution;
        this.lastSuccessfulExecution = lastSuccessfulExecution;
        this.lastThrottle = lastThrottle;
    }

    public ActionAckStatus ackStatus() {
        return ackStatus;
    }

    public ActionStatusExecution lastExecution() {
        return lastExecution;
    }

    public ActionStatusExecution lastSuccessfulExecution() {
        return lastSuccessfulExecution;
    }

    public ActionStatusThrottle lastThrottle() {
        return lastThrottle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionStatus that = (ActionStatus) o;

        return Objects.equals(ackStatus, that.ackStatus) &&
                Objects.equals(lastExecution, that.lastExecution) &&
                Objects.equals(lastSuccessfulExecution, that.lastSuccessfulExecution) &&
                Objects.equals(lastThrottle, that.lastThrottle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }


    public static void writeTo(ActionStatus status, StreamOutput out) throws IOException {
        ActionAckStatus.writeTo(status.ackStatus, out);
        out.writeBoolean(status.lastExecution != null);
        if (status.lastExecution != null) {
            ActionStatusExecution.writeTo(status.lastExecution, out);
        }
        out.writeBoolean(status.lastSuccessfulExecution != null);
        if (status.lastSuccessfulExecution != null) {
            ActionStatusExecution.writeTo(status.lastSuccessfulExecution, out);
        }
        out.writeBoolean(status.lastThrottle != null);
        if (status.lastThrottle != null) {
            ActionStatusThrottle.writeTo(status.lastThrottle, out);
        }
    }

    public static ActionStatus readFrom(StreamInput in) throws IOException {
        ActionAckStatus ackStatus = ActionAckStatus.readFrom(in);
        ActionStatusExecution lastExecution = in.readBoolean() ? ActionStatusExecution.readFrom(in) : null;
        ActionStatusExecution lastSuccessfulExecution = in.readBoolean() ? ActionStatusExecution.readFrom(in) : null;
        ActionStatusThrottle lastThrottle = in.readBoolean() ? ActionStatusThrottle.readFrom(in) : null;
        return new ActionStatus(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ActionStatusField.ACK_STATUS.getPreferredName(), ackStatus, params);
        if (lastExecution != null) {
            builder.field(ActionStatusField.LAST_EXECUTION.getPreferredName(), lastExecution, params);
        }
        if (lastSuccessfulExecution != null) {
            builder.field(ActionStatusField.LAST_SUCCESSFUL_EXECUTION.getPreferredName(), lastSuccessfulExecution, params);
        }
        if (lastThrottle != null) {
            builder.field(ActionStatusField.LAST_THROTTLE.getPreferredName(), lastThrottle, params);
        }
        return builder.endObject();
    }

    public static ActionStatus parse(String watchId, String actionId, XContentParser parser) throws IOException {
        ActionAckStatus ackStatus = null;
        ActionStatusExecution lastExecution = null;
        ActionStatusExecution lastSuccessfulExecution = null;
        ActionStatusThrottle lastThrottle = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ActionStatusField.ACK_STATUS.match(currentFieldName, parser.getDeprecationHandler())) {
                ackStatus = ActionAckStatus.parse(watchId, actionId, parser);
            } else if (ActionStatusField.LAST_EXECUTION.match(currentFieldName, parser.getDeprecationHandler())) {
                lastExecution = ActionStatusExecution.parse(watchId, actionId, parser);
            } else if (ActionStatusField.LAST_SUCCESSFUL_EXECUTION.match(currentFieldName, parser.getDeprecationHandler())) {
                lastSuccessfulExecution = ActionStatusExecution.parse(watchId, actionId, parser);
            } else if (ActionStatusField.LAST_THROTTLE.match(currentFieldName, parser.getDeprecationHandler())) {
                lastThrottle = ActionStatusThrottle.parse(watchId, actionId, parser);
            } else {
                throw new ElasticsearchParseException("could not parse action status for [{}/{}]. unexpected field [{}]", watchId,
                        actionId, currentFieldName);
            }
        }
        if (ackStatus == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}/{}]. missing required field [{}]", watchId,
                    actionId, ActionStatusField.ACK_STATUS.getPreferredName());
        }
        return new ActionStatus(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }

    @Override
    public String toString() {
        return "ActionStatus{" +
                "ackStatus=" + ackStatus +
                ", lastExecution=" + lastExecution +
                ", lastSuccessfulExecution=" + lastSuccessfulExecution +
                ", lastThrottle=" + lastThrottle +
                '}';
    }
}
