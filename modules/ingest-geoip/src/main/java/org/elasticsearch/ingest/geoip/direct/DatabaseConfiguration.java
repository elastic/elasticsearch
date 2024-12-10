/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
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
import java.util.Arrays;
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
public record DatabaseConfiguration(String id, String name, Provider provider) implements Writeable, ToXContentObject {

    // id is a user selected signifier like 'my_domain_db'
    // name is the name of a file that can be downloaded (like 'GeoIP2-Domain')

    // a configuration will have a 'provider' like "maxmind", and that might have some more details,
    // for now, though the important thing is that the json has to have it even though we don't model it meaningfully in this class

    public DatabaseConfiguration {
        // these are invariants, not actual validation
        Objects.requireNonNull(id);
        Objects.requireNonNull(name);
        Objects.requireNonNull(provider);
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

    public static final Set<String> IPINFO_NAMES = Set.of(
        // these file names are from https://ipinfo.io/developers/database-filename-reference
        "asn", // "Free IP to ASN"
        "country", // "Free IP to Country"
        // "country_asn" // "Free IP to Country + IP to ASN", not supported at present
        "standard_asn", // commercial "ASN"
        "standard_location", // commercial "IP Geolocation"
        "standard_privacy" // commercial "Privacy Detection" (sometimes "Anonymous IP")
    );

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField MAXMIND = new ParseField(Maxmind.NAME);
    private static final ParseField IPINFO = new ParseField(Ipinfo.NAME);
    private static final ParseField WEB = new ParseField(Web.NAME);
    private static final ParseField LOCAL = new ParseField(Local.NAME);

    private static final ConstructingObjectParser<DatabaseConfiguration, String> PARSER = new ConstructingObjectParser<>(
        "database",
        false,
        (a, id) -> {
            String name = (String) a[0];
            Provider provider;

            // one and only one provider object must be present
            final long numNonNulls = Arrays.stream(a, 1, a.length).filter(Objects::nonNull).count();
            if (numNonNulls != 1) {
                throw new IllegalArgumentException("Exactly one provider object must be specified, but [" + numNonNulls + "] were found");
            }

            if (a[1] != null) {
                provider = (Maxmind) a[1];
            } else if (a[2] != null) {
                provider = (Ipinfo) a[2];
            } else if (a[3] != null) {
                provider = (Web) a[3];
            } else {
                provider = (Local) a[4];
            }
            return new DatabaseConfiguration(id, name, provider);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (parser, id) -> Maxmind.PARSER.apply(parser, null),
            MAXMIND
        );
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (parser, id) -> Ipinfo.PARSER.apply(parser, null), IPINFO);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (parser, id) -> Web.PARSER.apply(parser, null), WEB);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (parser, id) -> Local.PARSER.apply(parser, null), LOCAL);
    }

    public DatabaseConfiguration(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), readProvider(in));
    }

    private static Provider readProvider(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            return in.readNamedWriteable(Provider.class);
        } else {
            // prior to the above version, everything was always a maxmind, so this half of the if is logical
            return new Maxmind(in.readString());
        }
    }

    public static DatabaseConfiguration parse(XContentParser parser, String id) {
        return PARSER.apply(parser, id);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(name);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeNamedWriteable(provider);
        } else {
            if (provider instanceof Maxmind maxmind) {
                out.writeString(maxmind.accountId);
            } else {
                /*
                 * The existence of a non-Maxmind providers is gated on the feature get_database_configuration_action.multi_node, and
                 * get_database_configuration_action.multi_node is only available on or after
                 * TransportVersions.INGEST_GEO_DATABASE_PROVIDERS.
                 */
                assert false : "non-maxmind DatabaseConfiguration.Provider [" + provider.getWriteableName() + "]";
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field(provider.getWriteableName(), provider);
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

        // provider-specific name validation
        if (provider instanceof Maxmind) {
            if (MAXMIND_NAMES.contains(name) == false) {
                err.addValidationError("invalid name [" + name + "]: must be a supported name ([" + MAXMIND_NAMES + "])");
            }
        }
        if (provider instanceof Ipinfo) {
            if (IPINFO_NAMES.contains(name) == false) {
                err.addValidationError("invalid name [" + name + "]: must be a supported name ([" + IPINFO_NAMES + "])");
            }
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

    public boolean isReadOnly() {
        return provider.isReadOnly();
    }

    /**
      * A marker interface that all providers need to implement.
      */
    public interface Provider extends NamedWriteable, ToXContentObject {
        boolean isReadOnly();
    }

    public record Maxmind(String accountId) implements Provider {
        public static final String NAME = "maxmind";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public Maxmind {
            // this is an invariant, not actual validation
            Objects.requireNonNull(accountId);
        }

        private static final ParseField ACCOUNT_ID = new ParseField("account_id");

        private static final ConstructingObjectParser<Maxmind, Void> PARSER = new ConstructingObjectParser<>("maxmind", false, (a, id) -> {
            String accountId = (String) a[0];
            return new Maxmind(accountId);
        });

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), ACCOUNT_ID);
        }

        public Maxmind(StreamInput in) throws IOException {
            this(in.readString());
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

        @Override
        public boolean isReadOnly() {
            return false;
        }
    }

    public record Ipinfo() implements Provider {
        public static final String NAME = "ipinfo";

        // this'll become a ConstructingObjectParser once we accept the token (securely) in the json definition
        private static final ObjectParser<Ipinfo, Void> PARSER = new ObjectParser<>("ipinfo", Ipinfo::new);

        public Ipinfo(StreamInput in) throws IOException {
            this();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }
    }

    public record Local(String type) implements Provider {
        public static final String NAME = "local";

        private static final ParseField TYPE = new ParseField("type");

        private static final ConstructingObjectParser<Local, Void> PARSER = new ConstructingObjectParser<>("database", false, (a, id) -> {
            String type = (String) a[0];
            return new Local(type);
        });

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
        }

        public Local(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }
    }

    public record Web() implements Provider {
        public static final String NAME = "web";

        private static final ObjectParser<Web, Void> PARSER = new ObjectParser<>("database", Web::new);

        public Web(StreamInput in) throws IOException {
            this();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }
    }
}
