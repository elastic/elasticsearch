/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.annotation.PublicApi;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.Script;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.Version;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;



@PublicApi(since = "2.14.0")
public class DerivedField implements Writeable, ToXContentFragment {
    private final String name;
    private final String type;
    private final Script script;
    private String prefilterField;
    private Map<String, Object> properties;
    private Boolean ignoreMalformed;
    private String format;

    public DerivedField(String name, String type, Script script) {
        this.name = name;
        this.type = type;
        this.script = script;
    }

    public DerivedField(StreamInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        script = new Script(in);
        if (in.getVersion().onOrAfter(Version.V_2_15_0)) {
            if (in.readBoolean()) {
                properties = in.readMap();
            }
            prefilterField = in.readOptionalString();
            format = in.readOptionalString();
            ignoreMalformed = in.readOptionalBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        script.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_15_0)) {
            if (properties == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeMap(properties);
            }
            out.writeOptionalString(prefilterField);
            out.writeOptionalString(format);
            out.writeOptionalBoolean(ignoreMalformed);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", type);
        builder.field("script", script);
        if (properties != null) {
            builder.field("properties", properties);
        }
        if (prefilterField != null) {
            builder.field("prefilter_field", prefilterField);
        }
        if (format != null) {
            builder.field("format", format);
        }
        if (ignoreMalformed != null) {
            builder.field("ignore_malformed", ignoreMalformed);
        }
        builder.endObject();
        return builder;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Script getScript() {
        return script;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String getNestedFieldType(String fieldName) {
        if (properties == null || properties.isEmpty() || fieldName == null || fieldName.isEmpty()) {
            return null;
        }
        return (String) properties.get(fieldName);
    }

    public String getPrefilterField() {
        return prefilterField;
    }

    public String getFormat() {
        return format;
    }

    public boolean getIgnoreMalformed() {
        return Boolean.TRUE.equals(ignoreMalformed);
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public void setPrefilterField(String prefilterField) {
        this.prefilterField = prefilterField;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setIgnoreMalformed(boolean ignoreMalformed) {
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, script, prefilterField, properties, ignoreMalformed, format);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DerivedField other = (DerivedField) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(type, other.type)
            && Objects.equals(script, other.script)
            && Objects.equals(prefilterField, other.prefilterField)
            && Objects.equals(properties, other.properties)
            && Objects.equals(ignoreMalformed, other.ignoreMalformed)
            && Objects.equals(format, other.format);
    }


}
