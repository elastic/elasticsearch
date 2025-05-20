/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.action.admin.cluster.settings.RestClusterGetSettingsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public enum TestResponseParsers {
    ;

    private static final ConstructingObjectParser<RestClusterGetSettingsResponse, Void> REST_SETTINGS_RESPONSE_PARSER =
        new ConstructingObjectParser<>("cluster_get_settings_response", true, a -> {
            Settings defaultSettings = a[2] == null ? Settings.EMPTY : (Settings) a[2];
            return new RestClusterGetSettingsResponse((Settings) a[0], (Settings) a[1], defaultSettings);
        });
    static {
        REST_SETTINGS_RESPONSE_PARSER.declareObject(
            constructorArg(),
            (p, c) -> Settings.fromXContent(p),
            new ParseField(RestClusterGetSettingsResponse.PERSISTENT_FIELD)
        );
        REST_SETTINGS_RESPONSE_PARSER.declareObject(
            constructorArg(),
            (p, c) -> Settings.fromXContent(p),
            new ParseField(RestClusterGetSettingsResponse.TRANSIENT_FIELD)
        );
        REST_SETTINGS_RESPONSE_PARSER.declareObject(
            optionalConstructorArg(),
            (p, c) -> Settings.fromXContent(p),
            new ParseField(RestClusterGetSettingsResponse.DEFAULTS_FIELD)
        );
    }

    public static RestClusterGetSettingsResponse parseClusterSettingsResponse(XContentParser parser) {
        return REST_SETTINGS_RESPONSE_PARSER.apply(parser, null);
    }

    private static final ParseField ACKNOWLEDGED_FIELD = new ParseField(AcknowledgedResponse.ACKNOWLEDGED_KEY);

    public static <T extends AcknowledgedResponse> void declareAcknowledgedField(ConstructingObjectParser<T, Void> objectParser) {
        objectParser.declareField(
            constructorArg(),
            (parser, context) -> parser.booleanValue(),
            ACKNOWLEDGED_FIELD,
            ObjectParser.ValueType.BOOLEAN
        );
    }

    public static <T extends ShardsAcknowledgedResponse> void declareAcknowledgedAndShardsAcknowledgedFields(
        ConstructingObjectParser<T, Void> objectParser
    ) {
        declareAcknowledgedField(objectParser);
        objectParser.declareField(
            constructorArg(),
            (parser, context) -> parser.booleanValue(),
            ShardsAcknowledgedResponse.SHARDS_ACKNOWLEDGED,
            ObjectParser.ValueType.BOOLEAN
        );
    }

    private static final ConstructingObjectParser<CreateIndexResponse, Void> CREATE_INDEX_RESPONSE_PARSER = new ConstructingObjectParser<>(
        "create_index",
        true,
        args -> new CreateIndexResponse((boolean) args[0], (boolean) args[1], (String) args[2])
    );

    static {
        declareAcknowledgedAndShardsAcknowledgedFields(CREATE_INDEX_RESPONSE_PARSER);
        CREATE_INDEX_RESPONSE_PARSER.declareField(
            constructorArg(),
            (parser, context) -> parser.textOrNull(),
            CreateIndexResponse.INDEX,
            ObjectParser.ValueType.STRING
        );
    }

    public static CreateIndexResponse parseCreateIndexResponse(XContentParser parser) {
        return CREATE_INDEX_RESPONSE_PARSER.apply(parser, null);
    }

    /**
     * A generic parser that simply parses the acknowledged flag
     */
    private static final ConstructingObjectParser<Boolean, Void> ACKNOWLEDGED_FLAG_PARSER = new ConstructingObjectParser<>(
        "acknowledged_flag",
        true,
        args -> (Boolean) args[0]
    );

    static {
        ACKNOWLEDGED_FLAG_PARSER.declareField(
            constructorArg(),
            (parser, context) -> parser.booleanValue(),
            ACKNOWLEDGED_FIELD,
            ObjectParser.ValueType.BOOLEAN
        );
    }

    public static AcknowledgedResponse parseAcknowledgedResponse(XContentParser parser) {
        return AcknowledgedResponse.of(ACKNOWLEDGED_FLAG_PARSER.apply(parser, null));
    }
}
