/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A single specified document to be used for a {@link PinnedQueryBuilder} or query rules.
 */
public final class SpecifiedDocument implements ToXContentObject, Writeable {

    public static final TransportVersion OPTIONAL_INDEX_IN_DOCS_VERSION = TransportVersions.V_8_11_X;

    public static final String NAME = "specified_document";

    public static final ParseField INDEX_FIELD = new ParseField("_index");
    public static final ParseField ID_FIELD = new ParseField("_id");

    private final String index;
    private final String id;

    /**
     * Constructor for a given specified document request
     *
     * @param index the index where the document is located
     * @param id and its id
     */
    public SpecifiedDocument(String index, String id) {
        if (index != null && Regex.isSimpleMatchPattern(index)) {
            throw new IllegalArgumentException("Specified document index cannot contain wildcard expressions");
        }
        if (id == null) {
            throw new IllegalArgumentException("Specified document requires id to be non-null");
        }
        this.index = index;
        this.id = id;
    }

    /**
     * Read from a stream.
     */
    public SpecifiedDocument(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(OPTIONAL_INDEX_IN_DOCS_VERSION)) {
            index = in.readOptionalString();
        } else {
            index = in.readString();
        }
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(OPTIONAL_INDEX_IN_DOCS_VERSION)) {
            out.writeOptionalString(index);
        } else {
            if (index == null) {
                throw new IllegalArgumentException(
                    "[_index] needs to be specified for docs elements when cluster nodes are not in the same version"
                );
            }
            out.writeString(index);
        }
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.index != null) {
            builder.field(INDEX_FIELD.getPreferredName(), this.index);
        }
        builder.field(ID_FIELD.getPreferredName(), this.id);
        return builder.endObject();
    }

    public static final ConstructingObjectParser<SpecifiedDocument, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new SpecifiedDocument((String) a[0], (String) a[1])
    );

    static {
        PARSER.declareString(optionalConstructorArg(), INDEX_FIELD);
        PARSER.declareString(constructorArg(), ID_FIELD);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if ((o instanceof SpecifiedDocument) == false) {
            return false;
        }
        SpecifiedDocument other = (SpecifiedDocument) o;
        return Objects.equals(index, other.index) && Objects.equals(id, other.id);
    }

    public String id() {
        return id;
    }

    public String index() {
        return index;
    }
}
