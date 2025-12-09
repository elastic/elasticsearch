/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates view definitions as custom metadata inside ProjectMetadata within cluster state.
 */
public final class ViewMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
    public static final String TYPE = "esql_view";
    public static final List<NamedWriteableRegistry.Entry> ENTRIES = List.of(
        new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TYPE, ViewMetadata::readFromStream),
        new NamedWriteableRegistry.Entry(NamedDiff.class, TYPE, in -> ViewMetadata.readDiffFrom(Metadata.ProjectCustom.class, TYPE, in))
    );
    public static final ViewMetadata EMPTY = new ViewMetadata(Collections.emptyMap());

    private static final TransportVersion ESQL_VIEWS = TransportVersion.fromName("esql_views");
    private static final ParseField VIEWS = new ParseField("views");

    private final Map<String, View> views;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ViewMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "view_metadata",
        true,
        (args, ctx) -> new ViewMetadata((Map<String, View>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, View> views = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                views.put(name, View.fromXContent(p));
            }
            return views;
        }, VIEWS);
    }

    public static ViewMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static ViewMetadata readFromStream(StreamInput in) throws IOException {
        return new ViewMetadata(in.readMap(View::new));
    }

    public ViewMetadata() {
        this(Map.of());
    }

    public ViewMetadata(Map<String, View> views) {
        this.views = Collections.unmodifiableMap(views);
    }

    public Map<String, View> views() {
        return views;
    }

    @Nullable
    public View getView(String name) {
        return views.get(name);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_VIEWS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.views, StreamOutput::writeWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentObjectFields(VIEWS.getPreferredName(), views);
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
