/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates enrich policies as custom metadata inside cluster state.
 */
public final class ViewMetadata extends AbstractNamedDiffable<Metadata.ClusterCustom> implements Metadata.ClusterCustom {
    public static final String TYPE = "esql_view";

    static final ParseField VIEWS = new ParseField("views");

    public static final ViewMetadata EMPTY = new ViewMetadata(Collections.emptyMap());

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ViewMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "enrich_metadata",
        args -> new ViewMetadata((Map<String, View>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), View.PARSER, VIEWS);
    }

    public static ViewMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Map<String, View> views;

    public ViewMetadata(StreamInput in) throws IOException {
        this(in.readMap(View::new));
    }

    public ViewMetadata(Map<String, View> views) {
        this.views = Collections.unmodifiableMap(views);
    }

    public Map<String, View> views() {
        return views;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO set this to something sane, perhaps once the new TransportVersion mechanism is in place
        return TransportVersions.INDEX_SOURCE;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(views, StreamOutput::writeWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentObjectFieldObjects(VIEWS.getPreferredName(), views);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.API);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ViewMetadata that = (ViewMetadata) o;
        return views.equals(that.views);
    }

    @Override
    public int hashCode() {
        return Objects.hash(views);
    }

}
