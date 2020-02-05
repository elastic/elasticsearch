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
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * A {@link LifecycleAction} which sets the index's priority. The higher the priority, the faster the recovery.
 */
public class SetPriorityAction implements LifecycleAction, ToXContentObject {
    public static final String NAME = "set_priority";
    private static final ParseField RECOVERY_PRIORITY_FIELD = new ParseField("priority");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SetPriorityAction, Void> PARSER = new ConstructingObjectParser<>(NAME, true,
        a -> new SetPriorityAction((Integer) a[0]));

    //package private for testing
    final Integer recoveryPriority;

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.intValue()
            , RECOVERY_PRIORITY_FIELD, ObjectParser.ValueType.INT_OR_NULL);
    }

    public static SetPriorityAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public SetPriorityAction(@Nullable Integer recoveryPriority) {
        if (recoveryPriority != null && recoveryPriority < 0) {
            throw new IllegalArgumentException("[" + RECOVERY_PRIORITY_FIELD.getPreferredName() + "] must be 0 or greater");
        }
        this.recoveryPriority = recoveryPriority;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(RECOVERY_PRIORITY_FIELD.getPreferredName(), recoveryPriority);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetPriorityAction that = (SetPriorityAction) o;

        return recoveryPriority != null ? recoveryPriority.equals(that.recoveryPriority) : that.recoveryPriority == null;
    }

    @Override
    public int hashCode() {
        return recoveryPriority != null ? recoveryPriority.hashCode() : 0;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
