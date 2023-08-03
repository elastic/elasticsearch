/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * The {@link ComposableIndexTemplateMetadata} class is a custom {@link Metadata.Custom} implementation that
 * stores a map of ids to {@link ComposableIndexTemplate} templates.
 */
public class ComposableIndexTemplateMetadata implements Metadata.Custom {
    public static final String TYPE = "index_template";
    private static final ParseField INDEX_TEMPLATE = new ParseField("index_template");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ComposableIndexTemplateMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        a -> new ComposableIndexTemplateMetadata((Map<String, ComposableIndexTemplate>) a[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, ComposableIndexTemplate> templates = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                templates.put(name, ComposableIndexTemplate.parse(p));
            }
            return templates;
        }, INDEX_TEMPLATE);
    }

    private final Map<String, ComposableIndexTemplate> indexTemplates;

    public ComposableIndexTemplateMetadata(Map<String, ComposableIndexTemplate> templates) {
        this.indexTemplates = templates;
    }

    public ComposableIndexTemplateMetadata(StreamInput in) throws IOException {
        this.indexTemplates = in.readMap(ComposableIndexTemplate::new);
    }

    public static ComposableIndexTemplateMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public Map<String, ComposableIndexTemplate> indexTemplates() {
        return indexTemplates;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new ComposableIndexTemplateMetadataDiff((ComposableIndexTemplateMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ComposableIndexTemplateMetadataDiff(in);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_7_7_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.indexTemplates, StreamOutput::writeString, (outstream, val) -> val.writeTo(outstream));
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentValuesMap(INDEX_TEMPLATE.getPreferredName(), indexTemplates);
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
        ComposableIndexTemplateMetadata other = (ComposableIndexTemplateMetadata) obj;
        return Objects.equals(this.indexTemplates, other.indexTemplates);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class ComposableIndexTemplateMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, ComposableIndexTemplate>> indexTemplateDiff;

        ComposableIndexTemplateMetadataDiff(ComposableIndexTemplateMetadata before, ComposableIndexTemplateMetadata after) {
            this.indexTemplateDiff = DiffableUtils.diff(
                before.indexTemplates,
                after.indexTemplates,
                DiffableUtils.getStringKeySerializer()
            );
        }

        ComposableIndexTemplateMetadataDiff(StreamInput in) throws IOException {
            this.indexTemplateDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                ComposableIndexTemplate::new,
                ComposableIndexTemplate::readITV2DiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ComposableIndexTemplateMetadata(indexTemplateDiff.apply(((ComposableIndexTemplateMetadata) part).indexTemplates));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indexTemplateDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.V_7_7_0;
        }
    }
}
