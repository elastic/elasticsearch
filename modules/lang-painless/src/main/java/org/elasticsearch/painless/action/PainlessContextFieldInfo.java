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

package org.elasticsearch.painless.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.painless.lookup.PainlessField;

import java.io.IOException;

public class PainlessContextFieldInfo implements Writeable, ToXContentObject {

    public static final ParseField DECLARING = new ParseField("declaring");
    public static final ParseField NAME = new ParseField("name");
    public static final ParseField TYPE = new ParseField("type");

    private final String declaring;
    private final String name;
    private final String type;

    public PainlessContextFieldInfo(PainlessField painlessField) {
        this(
                painlessField.javaField.getDeclaringClass().getName(),
                painlessField.javaField.getName(),
                painlessField.typeParameter.getName()
        );
    }

    public PainlessContextFieldInfo(String declaring, String name, String type) {
        this.declaring = declaring;
        this.name = name;
        this.type = type;
    }

    public PainlessContextFieldInfo(StreamInput in) throws IOException {
        declaring = in.readString();
        name = in.readString();
        type = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(declaring);
        out.writeString(name);
        out.writeString(type);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DECLARING.getPreferredName(), declaring);
        builder.field(NAME.getPreferredName(), name);
        builder.field(TYPE.getPreferredName(), type);
        builder.endObject();

        return builder;
    }
}
