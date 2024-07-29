/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A database configuration is an identified (has an id) configuration of a named geoip location database to download,
 * and the identifying information/configuration to download the named database from some database provider.
 * <p>
 * That is, it has an id e.g. "my_db_config_1" and it says "download the file named XXXX from SomeCompany, and here's the
 * magic token to use to do that."
 */
public record DatabaseConfiguration(String id, String name, Maxmind maxmind) implements Writeable, ToXContentObject {

    // id is a user selected signifier like 'my_domain_db'
    // name is the name of a file that can be downloaded (like 'GeoIP2-Domain')

    // a configuration will have a 'type' like "maxmind", and that might have some more details,
    // for now, though the important thing is that the json has to have it even though we don't model it meaningfully in this class

    public DatabaseConfiguration {
        // these are invariants, not actual validation
        Objects.requireNonNull(id);
        Objects.requireNonNull(name);
        Objects.requireNonNull(maxmind);
    }

    /**
     * An alphanumeric, followed by 0-126 alphanumerics, dashes, or underscores. That is, 1-127 alphanumerics, dashes, or underscores,
     * but a leading dash or underscore isn't allowed (we're reserving leading dashes and underscores [and other odd characters] for
     * Elastic and the future).
     */
    private static final Pattern ID_PATTERN = Pattern.compile("\\p{Alnum}[_\\-\\p{Alnum}]{0,126}");

    public static final Set<String> MAXMIND_NAMES = Set.of(
        "GeoIP2-Anonymous-IP",
        "GeoIP2-City",
        "GeoIP2-Connection-Type",
        "GeoIP2-Country",
        "GeoIP2-Domain",
        "GeoIP2-Enterprise",
        "GeoIP2-ISP"

        // in order to prevent a conflict between the (ordinary) geoip downloader and the enterprise geoip downloader,
        // the enterprise geoip downloader is limited only to downloading the commercial files that the (ordinary) geoip downloader
        // doesn't support out of the box -- in the future if we would like to relax this constraint, then we'll need to resolve that
        // conflict at the same time.

        // "GeoLite2-ASN",
        // "GeoLite2-City",
        // "GeoLite2-Country"
    );

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField MAXMIND = new ParseField("maxmind");

    private static final ConstructingObjectParser<DatabaseConfiguration, String> PARSER = new ConstructingObjectParser<>(
        "database",
        false,
        (a, id) -> {
            String name = (String) a[0];
            Maxmind maxmind = (Maxmind) a[1];
            return new DatabaseConfiguration(id, name, maxmind);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (parser, id) -> Maxmind.PARSER.apply(parser, null), MAXMIND);
    }

    public DatabaseConfiguration(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), new Maxmind(in));
    }

    public static DatabaseConfiguration parse(XContentParser parser, String id) {
        return PARSER.apply(parser, id);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(name);
        maxmind.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("maxmind", maxmind);
        builder.endObject();
        return builder;
    }

    /**
     * An id is intended to be alphanumerics, dashes, and underscores (only), but we're reserving leading dashes and underscores for
     * ourselves in the future, that is, they're not for the ones that users can PUT.
     */
    static void validateId(String id) throws IllegalArgumentException {
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("invalid database configuration id [" + id + "]: must not be null or empty");
        }
        MetadataCreateIndexService.validateIndexOrAliasName(
            id,
            (id1, description) -> new IllegalArgumentException("invalid database configuration id [" + id1 + "]: " + description)
        );
        int byteCount = id.getBytes(StandardCharsets.UTF_8).length;
        if (byteCount > 127) {
            throw new IllegalArgumentException(
                "invalid database configuration id [" + id + "]: id is too long, (" + byteCount + " > " + 127 + ")"
            );
        }
        if (ID_PATTERN.matcher(id).matches() == false) {
            throw new IllegalArgumentException(
                "invalid database configuration id ["
                    + id
                    + "]: id doesn't match required rules (alphanumerics, dashes, and underscores, only)"
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
        try {
            validateId(id);
        } catch (IllegalArgumentException e) {
            err.addValidationError(e.getMessage());
        }
        return err.validationErrors().isEmpty() ? null : err;
    }

    public record Maxmind(String accountId) implements Writeable, ToXContentObject {

        public Maxmind {
            // this is an invariant, not actual validation
            Objects.requireNonNull(accountId);
        }

        private static final ParseField ACCOUNT_ID = new ParseField("account_id");

        private static final ConstructingObjectParser<Maxmind, Void> PARSER = new ConstructingObjectParser<>("database", false, (a, id) -> {
            String accountId = (String) a[0];
            return new Maxmind(accountId);
        });

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), ACCOUNT_ID);
        }

        public Maxmind(StreamInput in) throws IOException {
            this(in.readString());
        }

        public static Maxmind parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(accountId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("account_id", accountId);
            builder.endObject();
            return builder;
        }
    }
}
