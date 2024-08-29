/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * {@code DatabaseConfigurationMetadata} encapsulates a {@link DatabaseConfiguration} as well as
 * the additional meta information like version (a monotonically incrementing number), and last modified date.
 */
public record DatabaseConfigurationMetadata(DatabaseConfiguration database, long version, long modifiedDate)
    implements
        SimpleDiffable<DatabaseConfigurationMetadata>,
        ToXContentObject {

    public static final ParseField DATABASE = new ParseField("database");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField MODIFIED_DATE_MILLIS = new ParseField("modified_date_millis");
    public static final ParseField MODIFIED_DATE = new ParseField("modified_date");
    // later, things like this:
    // static final ParseField LAST_SUCCESS = new ParseField("last_success");
    // static final ParseField LAST_FAILURE = new ParseField("last_failure");

    public static final ConstructingObjectParser<DatabaseConfigurationMetadata, String> PARSER = new ConstructingObjectParser<>(
        "database_metadata",
        true,
        a -> {
            DatabaseConfiguration database = (DatabaseConfiguration) a[0];
            return new DatabaseConfigurationMetadata(database, (long) a[1], (long) a[2]);
        }
    );
    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), DatabaseConfiguration::parse, DATABASE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_MILLIS);
    }

    public static DatabaseConfigurationMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    public DatabaseConfigurationMetadata(StreamInput in) throws IOException {
        this(new DatabaseConfiguration(in), in.readVLong(), in.readVLong());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // this is cluster state serialization, the id is implicit and doesn't need to included here
        // (we'll be a in a json map where the id is the key)
        builder.startObject();
        builder.field(VERSION.getPreferredName(), version);
        builder.timeField(MODIFIED_DATE_MILLIS.getPreferredName(), MODIFIED_DATE.getPreferredName(), modifiedDate);
        builder.field(DATABASE.getPreferredName(), database);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        database.writeTo(out);
        out.writeVLong(version);
        out.writeVLong(modifiedDate);
    }

    public static Diff<DatabaseConfigurationMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DatabaseConfigurationMetadata::new, in);
    }
}
