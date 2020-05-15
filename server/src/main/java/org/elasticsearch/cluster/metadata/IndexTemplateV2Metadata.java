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
 * The {@link IndexTemplateV2Metadata} class is a custom {@link Metadata.Custom} implementation that
 * stores a map of ids to {@link IndexTemplateV2} templates.
 */
public class IndexTemplateV2Metadata implements Metadata.Custom {
    public static final String TYPE = "index_template";
    private static final ParseField INDEX_TEMPLATE = new ParseField("index_template");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexTemplateV2Metadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false,
        a -> new IndexTemplateV2Metadata((Map<String, IndexTemplateV2>) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, IndexTemplateV2> templates = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                templates.put(name, IndexTemplateV2.parse(p));
            }
            return templates;
        }, INDEX_TEMPLATE);
    }

    private final Map<String, IndexTemplateV2> indexTemplates;

    public IndexTemplateV2Metadata(Map<String, IndexTemplateV2> templates) {
        this.indexTemplates = templates;
    }

    public IndexTemplateV2Metadata(StreamInput in) throws IOException {
        this.indexTemplates = in.readMap(StreamInput::readString, IndexTemplateV2::new);
    }

    public static IndexTemplateV2Metadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public Map<String, IndexTemplateV2> indexTemplates() {
        return indexTemplates;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new IndexTemplateV2MetadataDiff((IndexTemplateV2Metadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new IndexTemplateV2MetadataDiff(in);
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
        out.writeMap(this.indexTemplates, StreamOutput::writeString, (outstream, val) -> val.writeTo(outstream));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(INDEX_TEMPLATE.getPreferredName());
        for (Map.Entry<String, IndexTemplateV2> template : indexTemplates.entrySet()) {
            builder.field(template.getKey(), template.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.indexTemplates);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IndexTemplateV2Metadata other = (IndexTemplateV2Metadata) obj;
        return Objects.equals(this.indexTemplates, other.indexTemplates);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class IndexTemplateV2MetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, IndexTemplateV2>> indexTemplateDiff;

        IndexTemplateV2MetadataDiff(IndexTemplateV2Metadata before, IndexTemplateV2Metadata after) {
            this.indexTemplateDiff = DiffableUtils.diff(before.indexTemplates, after.indexTemplates,
                DiffableUtils.getStringKeySerializer());
        }

        IndexTemplateV2MetadataDiff(StreamInput in) throws IOException {
            this.indexTemplateDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(),
                IndexTemplateV2::new, IndexTemplateV2::readITV2DiffFrom);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new IndexTemplateV2Metadata(indexTemplateDiff.apply(((IndexTemplateV2Metadata) part).indexTemplates));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indexTemplateDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
