/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.core.RestApiVersion.equalTo;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Encapsulates the parameters needed to fetch terms.
 */
public class TermsLookup implements Writeable, ToXContentFragment {

    private final String index;
    private final String id;
    private final String path;
    private String routing;

    public TermsLookup(String index, String id, String path) {
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
        this.id = id;
        this.path = path;
    }

    /**
     * Read from a stream.
     */
    public TermsLookup(StreamInput in) throws IOException {
        if (in.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            in.readOptionalString();
        }
        id = in.readString();
        path = in.readString();
        index = in.readString();
        routing = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeString(path);
        out.writeString(index);
        out.writeOptionalString(routing);
    }

    public String index() {
        return index;
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
        String id = (String) args[1];
        String path = (String) args[2];
        return new TermsLookup(index, id, path);
    });
    static {
        PARSER.declareString(constructorArg(), new ParseField("index"));
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("path"));
        PARSER.declareString(TermsLookup::routing, new ParseField("routing"));
        PARSER.declareString((termLookup, type) -> {}, new ParseField("type").forRestApiVersion(equalTo(RestApiVersion.V_7)));
    }

    public static TermsLookup parseTermsLookup(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return index + "/" + id + "/" + path;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("index", index);
        builder.field("id", id);
        builder.field("path", path);
        if (routing != null) {
            builder.field("routing", routing);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id, path, routing);
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
            && Objects.equals(id, other.id)
            && Objects.equals(path, other.path)
            && Objects.equals(routing, other.routing);
    }
}
