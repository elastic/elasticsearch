/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities for parsing field_caps responses for test purposes.
 */
public enum FieldCapsUtils {
    ;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldCapabilitiesFailure, Void> FAILURE_PARSER = new ConstructingObjectParser<>(
        "field_capabilities_failure",
        true,
        a -> new FieldCapabilitiesFailure(((List<String>) a[0]).toArray(String[]::new), (Exception) a[1])
    );

    static {
        FAILURE_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), FieldCapabilitiesFailure.INDICES_FIELD);
        FAILURE_PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.currentToken(), p);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.nextToken(), p);
            Exception e = ElasticsearchException.failureFromXContent(p);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, p.nextToken(), p);
            return e;
        }, FieldCapabilitiesFailure.FAILURE_FIELD);
    }

    public static FieldCapabilitiesFailure parseFailure(XContentParser parser) throws IOException {
        return FAILURE_PARSER.parse(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldCapabilitiesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "field_capabilities_response",
        true,
        a -> {
            Map<String, Map<String, FieldCapabilities>> responseMap = ((List<Tuple<String, Map<String, FieldCapabilities>>>) a[0]).stream()
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));
            List<String> indices = a[1] == null ? Collections.emptyList() : (List<String>) a[1];
            List<FieldCapabilitiesFailure> failures = a[2] == null ? Collections.emptyList() : (List<FieldCapabilitiesFailure>) a[2];
            return new FieldCapabilitiesResponse(indices.toArray(String[]::new), responseMap, failures);
        }
    );

    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> {
            Map<String, FieldCapabilities> typeToCapabilities = parseTypeToCapabilities(p, n);
            return new Tuple<>(n, typeToCapabilities);
        }, FieldCapabilitiesResponse.FIELDS_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilitiesResponse.INDICES_FIELD);
        PARSER.declareObjectArray(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> parseFailure(p),
            FieldCapabilitiesResponse.FAILURES_FIELD
        );
    }

    public static FieldCapabilitiesResponse parseFieldCapsResponse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static Map<String, FieldCapabilities> parseTypeToCapabilities(XContentParser parser, String name) throws IOException {
        Map<String, FieldCapabilities> typeToCapabilities = new HashMap<>();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String type = parser.currentName();
            FieldCapabilities capabilities = parseFieldCaps(name, parser);
            typeToCapabilities.put(type, capabilities);
        }
        return typeToCapabilities;
    }

    public static FieldCapabilities parseFieldCaps(String name, XContentParser parser) throws IOException {
        return FIELD_CAPS_PARSER.parse(parser, name);
    }

    private static final InstantiatingObjectParser<FieldCapabilities, String> FIELD_CAPS_PARSER;

    static {
        InstantiatingObjectParser.Builder<FieldCapabilities, String> parser = InstantiatingObjectParser.builder(
            "field_capabilities",
            true,
            FieldCapabilities.class
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), FieldCapabilities.TYPE_FIELD);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilities.IS_METADATA_FIELD);
        parser.declareBoolean(ConstructingObjectParser.constructorArg(), FieldCapabilities.SEARCHABLE_FIELD);
        parser.declareBoolean(ConstructingObjectParser.constructorArg(), FieldCapabilities.AGGREGATABLE_FIELD);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilities.TIME_SERIES_DIMENSION_FIELD);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilities.TIME_SERIES_METRIC_FIELD);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilities.INDICES_FIELD);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilities.NON_SEARCHABLE_INDICES_FIELD);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilities.NON_AGGREGATABLE_INDICES_FIELD);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilities.NON_DIMENSION_INDICES_FIELD);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), FieldCapabilities.METRIC_CONFLICTS_INDICES_FIELD);
        parser.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, context) -> p.map(HashMap::new, v -> Set.copyOf(v.list())),
            new ParseField("meta")
        );
        FIELD_CAPS_PARSER = parser.build();
    }
}
