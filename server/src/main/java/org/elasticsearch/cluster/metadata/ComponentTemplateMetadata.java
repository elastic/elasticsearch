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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link ComponentTemplateMetadata} is a custom {@link Metadata} implementation for storing a map
 * of component templates and their names.
 */
public class ComponentTemplateMetadata implements Metadata.Custom {
    public static final String TYPE = "component_template";
    private static final ParseField COMPONENT_TEMPLATE = new ParseField("component_template");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ComponentTemplateMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false,
        a -> new ComponentTemplateMetadata((Map<String, ComponentTemplate>) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, ComponentTemplate> templates = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                templates.put(name, ComponentTemplate.parse(p));
            }
            return templates;
        }, COMPONENT_TEMPLATE);
    }
    private final Map<String, ComponentTemplate> componentTemplates;

    public ComponentTemplateMetadata(Map<String, ComponentTemplate> componentTemplates) {
        this.componentTemplates = componentTemplates;
    }

    public ComponentTemplateMetadata(StreamInput in) throws IOException {
        this.componentTemplates = in.readMap(StreamInput::readString, ComponentTemplate::new);
    }

    public Map<String, ComponentTemplate> componentTemplates() {
        return this.componentTemplates;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new ComponentTemplateMetadataDiff((ComponentTemplateMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ComponentTemplateMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_7_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.componentTemplates, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    public static ComponentTemplateMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(COMPONENT_TEMPLATE.getPreferredName());
        for (Map.Entry<String, ComponentTemplate> template : componentTemplates.entrySet()) {
            builder.field(template.getKey(), template.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.componentTemplates);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ComponentTemplateMetadata other = (ComponentTemplateMetadata) obj;
        return Objects.equals(this.componentTemplates, other.componentTemplates);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class ComponentTemplateMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, ComponentTemplate>> componentTemplateDiff;

        ComponentTemplateMetadataDiff(ComponentTemplateMetadata before, ComponentTemplateMetadata after) {
            this.componentTemplateDiff = DiffableUtils.diff(before.componentTemplates, after.componentTemplates,
                DiffableUtils.getStringKeySerializer());
        }

        ComponentTemplateMetadataDiff(StreamInput in) throws IOException {
            this.componentTemplateDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(),
                ComponentTemplate::new, ComponentTemplate::readComponentTemplateDiffFrom);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ComponentTemplateMetadata(componentTemplateDiff.apply(((ComponentTemplateMetadata) part).componentTemplates));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            componentTemplateDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
