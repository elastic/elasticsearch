/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.io.stream.StreamOutput.GENERIC_LIST_HEADER;

/**
 * Encapsulates view definitions as custom metadata inside ProjectMetadata within cluster state.
 */
public final class ViewMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
    public static final String TYPE = "esql_view";
    public static final List<NamedWriteableRegistry.Entry> ENTRIES = List.of(
        new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TYPE, ViewMetadata::readFromStream),
        new NamedWriteableRegistry.Entry(NamedDiff.class, TYPE, in -> ViewMetadata.readDiffFrom(Metadata.ProjectCustom.class, TYPE, in))
    );
    private static final TransportVersion ESQL_VIEWS = TransportVersion.fromName("esql_views");

    static final ParseField VIEWS = new ParseField("views");

    public static final ViewMetadata EMPTY = new ViewMetadata(Collections.emptyList());

    @SuppressWarnings("unchecked")
    private static final ObjectParser<ViewMetadata, Void> PARSER = new ObjectParser<>("view_metadata", ViewMetadata::new);

    static {
        PARSER.declareObjectArrayOrNull((viewMetadata, views) -> views.forEach(viewMetadata::add), (p, c) -> View.fromXContent(p), VIEWS);
    }

    public static ViewMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final ArrayList<View> views;

    public static ViewMetadata readFromStream(StreamInput in) throws IOException {
        assert in.readByte() == GENERIC_LIST_HEADER;
        int count = in.readVInt();
        ArrayList<View> views = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            views.add(new View(in));
        }
        return new ViewMetadata(views);
    }

    public ViewMetadata() {
        this(List.of());
    }

    public ViewMetadata(List<View> views) {
        this.views = new ArrayList<>(views);
    }

    public void add(View view) {
        views.add(view);
    }

    public List<View> views() {
        return views;
    }

    public List<String> viewNames() {
        return views.stream().map(View::name).toList();
    }

    public View getView(String name) {
        return views.stream().filter(v -> v.name().equals(name)).findAny().orElse(null);
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
        out.writeGenericList(views, StreamOutput::writeWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.array(VIEWS.getPreferredName(), new ViewIterator());
    }

    private class ViewIterator implements Iterator<ToXContent> {
        private final Iterator<View> internal = views.iterator();

        @Override
        public boolean hasNext() {
            return internal.hasNext();
        }

        @Override
        public ToXContent next() {
            View view = internal.next();
            return (builder, params) -> {
                builder.startObject();
                view.toXContent(builder, params);
                builder.endObject();
                return builder;
            };
        }
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
