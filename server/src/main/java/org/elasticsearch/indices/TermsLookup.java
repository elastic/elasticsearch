/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Encapsulates the parameters needed to fetch terms.
 */
public class TermsLookup implements Writeable, ToXContentFragment {

    private final String index;
    private @Nullable String type;
    private final String id;
    private final String path;
    private String routing;

    public TermsLookup(String index, String id, String path) {
        this(index, null, id, path);
    }

    /**
     * @deprecated Types are in the process of being removed, use {@link TermsLookup(String, String, String)} instead.
     */
    @Deprecated
    public TermsLookup(String index, String type, String id, String path) {
        if (id == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the id.");
        }
        if (path == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the path.");
        }
        if (index == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the index.");
        }
        this.index = index;
        this.type = type;
        this.id = id;
        this.path = path;
    }

    /**
     * Read from a stream.
     */
    public TermsLookup(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            type = in.readOptionalString();
        } else {
            // Before 7.0, the type parameter was always non-null and serialized as a (non-optional) string.
            type = in.readString();
        }
        id = in.readString();
        path = in.readString();
        if (in.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
            index = in.readString();
        } else {
            index = in.readOptionalString();
            if (index == null) {
                throw new IllegalStateException("index must not be null in a terms lookup");
            }
        }
        routing = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeOptionalString(type);
        } else {
            if (type == null) {
                throw new IllegalArgumentException(
                    "Typeless [terms] lookup queries are not supported if any " + "node is running a version before 7.0."
                );

            }
            out.writeString(type);
        }
        out.writeString(id);
        out.writeString(path);
        if (out.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
            out.writeString(index);
        } else {
            out.writeOptionalString(index);
        }
        out.writeOptionalString(routing);
    }

    public String index() {
        return index;
    }

    /**
     * @deprecated Types are in the process of being removed.
     */
    @Deprecated
    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public String path() {
        return path;
    }

    public String routing() {
        return routing;
    }

    public TermsLookup routing(String routing) {
        this.routing = routing;
        return this;
    }

    private static final ConstructingObjectParser<TermsLookup, Void> PARSER = new ConstructingObjectParser<>("terms_lookup", args -> {
        String index = (String) args[0];
        String type = (String) args[1];
        String id = (String) args[2];
        String path = (String) args[3];
        return new TermsLookup(index, type, id, path);
    });
    static {
        PARSER.declareString(constructorArg(), new ParseField("index"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("type").withAllDeprecated());
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("path"));
        PARSER.declareString(TermsLookup::routing, new ParseField("routing"));
    }

    public static TermsLookup parseTermsLookup(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        if (type == null) {
            return index + "/" + id + "/" + path;
        } else {
            return index + "/" + type + "/" + id + "/" + path;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("index", index);
        if (type != null) {
            builder.field("type", type);
        }
        builder.field("id", id);
        builder.field("path", path);
        if (routing != null) {
            builder.field("routing", routing);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id, path, routing);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TermsLookup other = (TermsLookup) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(type, other.type)
            && Objects.equals(id, other.id)
            && Objects.equals(path, other.path)
            && Objects.equals(routing, other.routing);
    }
}
