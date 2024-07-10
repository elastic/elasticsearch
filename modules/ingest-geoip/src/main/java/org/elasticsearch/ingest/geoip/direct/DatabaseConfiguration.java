/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * TODO javadocs
 */
public record DatabaseConfiguration(String id, String name) implements Writeable, ToXContentObject {

    // id is a user selected signifier like 'my_domain_db'
    // name is the name of a file that can be downloaded (like 'GeoIP2-Domain')

    // a configuration will have a 'type' like "maxmind", and that might have some more details,
    // for now, though the important thing is that the json has to have it even though we don't model it meaningfully in this class

    public DatabaseConfiguration {
        // these are invariants, not actual validation
        Objects.requireNonNull(id);
        Objects.requireNonNull(name);
    }

    /**
     * An alphanumeric, followed by 0-126 alphanumerics or underscores. That is, 1-127 alphanumerics or underscores, but a leading
     * underscore isn't allowed (we're reserving leading underscores [and other odd characters] for Elastic and the future).
     */
    private static final Pattern ID_PATTERN = Pattern.compile("\\p{Alnum}[_\\p{Alnum}]{0,126}");

    public static final Set<String> MAXMIND_NAMES = Set.of(
        "GeoIP2-Anonymous-IP",
        "GeoIP2-City",
        "GeoIP2-Connection-Type",
        "GeoIP2-Country",
        "GeoIP2-Domain",
        "GeoIP2-Enterprise",
        "GeoIP2-ISP",
        "GeoLite2-ASN",
        "GeoLite2-City",
        "GeoLite2-Country"
    );

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField MAXMIND = new ParseField("maxmind");

    private static final ConstructingObjectParser<DatabaseConfiguration, String> PARSER = new ConstructingObjectParser<>(
        "database",
        false,
        (a, id) -> {
            String name = (String) a[0];
            Object ignoredMaxmind = a[1];
            return new DatabaseConfiguration(id, name);
        }
    );

    public static final ObjectParser<Object, String> MAXMIND_PARSER = new ObjectParser<>("maxmind", Object::new);

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), MAXMIND_PARSER::apply, MAXMIND);
    }

    public DatabaseConfiguration(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    public static DatabaseConfiguration parse(XContentParser parser, String id) {
        return PARSER.apply(parser, id);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(name);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("maxmind", Map.of()); // we serialize an empty map here for json future-proofing
        builder.endObject();
        return builder;
    }

    /**
     * TODO javadocs
     */
    public static void validateId(String id) {
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("invalid database configuration id [" + id + "]: must not be null or empty");
        }
        if (id.contains(" ")) {
            throw new IllegalArgumentException("invalid database configuration id [" + id + "]: must not contain spaces");
        }
        if (id.contains(",")) {
            throw new IllegalArgumentException("invalid database configuration id [" + id + "]: must not contain ','");
        }
        if (id.contains("-")) {
            throw new IllegalArgumentException("invalid database configuration id [" + id + "]: must not contain '-'");
        }
        if (id.charAt(0) == '_') {
            throw new IllegalArgumentException("invalid database configuration id [" + id + "]: must not start with '_'");
        }
        int byteCount = id.getBytes(StandardCharsets.UTF_8).length;
        if (byteCount > 127) {
            throw new IllegalArgumentException(
                "invalid database configuration id [" + id + "]: name is too long, (" + byteCount + " > " + 127 + ")"
            );
        }
        if (ID_PATTERN.matcher(id).matches() == false) {
            throw new IllegalArgumentException(
                // TODO a better validation message here might be nice
                "invalid database configuration id [" + id + "]: name doesn't match required rules (alphanumerics and underscores, only)"
            );
        }
    }

    public ActionRequestValidationException validate() {
        ActionRequestValidationException err = new ActionRequestValidationException();

        // how do we cross the id validation divide here? or do we? it seems unfortunate to not invoke it at all.

        // name validation
        if (Strings.hasText(name) == false) {
            err.addValidationError("invalid name [" + name + "]: cannot be empty");
        }

        if (MAXMIND_NAMES.contains(name) == false) {
            err.addValidationError("invalid name [" + name + "]: must be a supported name ([" + MAXMIND_NAMES + "])");
        }

        // important: the name must be unique across all configurations of this same type,
        // but we validate that in the cluster state update, not here.

        return err.validationErrors().isEmpty() ? null : err;
    }
}
