/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultMappingParametersHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
        var fieldType = FieldType.tryParse(request.fieldType());
        if (fieldType == null) {
            // This is a custom field type
            return null;
        }

        return new DataSourceResponse.LeafMappingParametersGenerator(switch (fieldType) {
            case KEYWORD -> keywordMapping(request);
            case LONG, INTEGER, SHORT, BYTE, DOUBLE, FLOAT, HALF_FLOAT, UNSIGNED_LONG -> numberMapping(fieldType);
            case SCALED_FLOAT -> scaledFloatMapping();
            case COUNTED_KEYWORD -> countedKeywordMapping();
            case BOOLEAN -> booleanMapping();
            case DATE -> dateMapping();
            case GEO_POINT -> geoPointMapping();
            case TEXT -> textMapping(request);
            case IP -> ipMapping();
            case CONSTANT_KEYWORD -> constantKeywordMapping();
            case WILDCARD -> wildcardMapping();
        });
    }

    private Supplier<Map<String, Object>> numberMapping(FieldType fieldType) {
        return () -> {
            var mapping = commonMappingParameters();

            if (ESTestCase.randomBoolean()) {
                mapping.put("ignore_malformed", ESTestCase.randomBoolean());
            }
            if (ESTestCase.randomDouble() <= 0.2) {
                Number value = switch (fieldType) {
                    case LONG -> ESTestCase.randomLong();
                    case UNSIGNED_LONG -> ESTestCase.randomNonNegativeLong();
                    case INTEGER -> ESTestCase.randomInt();
                    case SHORT -> ESTestCase.randomShort();
                    case BYTE -> ESTestCase.randomByte();
                    case DOUBLE -> ESTestCase.randomDouble();
                    case FLOAT, HALF_FLOAT -> ESTestCase.randomFloat();
                    default -> throw new IllegalStateException("Unexpected field type");
                };

                mapping.put("null_value", value);
            }

            return mapping;
        };
    }

    private Supplier<Map<String, Object>> keywordMapping(DataSourceRequest.LeafMappingParametersGenerator request) {
        return () -> {
            var mapping = commonMappingParameters();

            // Inject copy_to sometimes but reflect that it is not widely used in reality.
            // We only add copy_to to keywords because we get into trouble with numeric fields that are copied to dynamic fields.
            // If first copied value is numeric, dynamic field is created with numeric field type and then copy of text values fail.
            // Actual value being copied does not influence the core logic of copy_to anyway.
            if (ESTestCase.randomDouble() <= 0.05) {
                var options = request.eligibleCopyToFields()
                    .stream()
                    .filter(f -> f.equals(request.fieldName()) == false)
                    .collect(Collectors.toSet());

                if (options.isEmpty() == false) {
                    mapping.put("copy_to", ESTestCase.randomFrom(options));
                }
            }

            if (ESTestCase.randomDouble() <= 0.2) {
                mapping.put("ignore_above", ESTestCase.randomIntBetween(1, 100));
            }
            if (ESTestCase.randomDouble() <= 0.2) {
                mapping.put("null_value", ESTestCase.randomAlphaOfLengthBetween(0, 10));
            }
            // NOCOMMIT - randomize this
            injected.put("normalizer", "lowercase");
            return mapping;
        };
    }

    private Supplier<Map<String, Object>> scaledFloatMapping() {
        return () -> {
            var mapping = commonMappingParameters();

            mapping.put("scaling_factor", ESTestCase.randomFrom(10, 1000, 100000, 100.5));

            if (ESTestCase.randomDouble() <= 0.2) {
                mapping.put("null_value", ESTestCase.randomDouble());
            }

            if (ESTestCase.randomBoolean()) {
                mapping.put("ignore_malformed", ESTestCase.randomBoolean());
            }

            return mapping;
        };
    }

    private Supplier<Map<String, Object>> countedKeywordMapping() {
        return () -> Map.of("index", ESTestCase.randomBoolean());
    }

    private Supplier<Map<String, Object>> booleanMapping() {
        return () -> {
            var mapping = commonMappingParameters();

            if (ESTestCase.randomDouble() <= 0.2) {
                mapping.put("null_value", ESTestCase.randomFrom(true, false, "true", "false"));
            }

            if (ESTestCase.randomBoolean()) {
                mapping.put("ignore_malformed", ESTestCase.randomBoolean());
            }

            return mapping;
        };
    }

    // just a custom format, specific format does not matter
    private static final String FORMAT = "yyyy_MM_dd_HH_mm_ss_n";

    private Supplier<Map<String, Object>> dateMapping() {
        return () -> {
            var mapping = commonMappingParameters();

            String format = null;
            if (ESTestCase.randomBoolean()) {
                format = FORMAT;
                mapping.put("format", format);
            }

            if (ESTestCase.randomDouble() <= 0.2) {
                var instant = ESTestCase.randomInstantBetween(Instant.parse("2300-01-01T00:00:00Z"), Instant.parse("2350-01-01T00:00:00Z"));

                if (format == null) {
                    mapping.put("null_value", instant.toEpochMilli());
                } else {
                    mapping.put(
                        "null_value",
                        DateTimeFormatter.ofPattern(format, Locale.ROOT).withZone(ZoneId.from(ZoneOffset.UTC)).format(instant)
                    );
                }
            }

            if (ESTestCase.randomBoolean()) {
                mapping.put("ignore_malformed", ESTestCase.randomBoolean());
            }

            return mapping;
        };
    }

    private Supplier<Map<String, Object>> geoPointMapping() {
        return () -> {
            var mapping = commonMappingParameters();

            if (ESTestCase.randomDouble() <= 0.2) {
                var point = GeometryTestUtils.randomPoint(false);
                mapping.put("null_value", WellKnownText.toWKT(point));
            }

            if (ESTestCase.randomBoolean()) {
                mapping.put("ignore_malformed", ESTestCase.randomBoolean());
            }

            return mapping;
        };
    }

    private Supplier<Map<String, Object>> textMapping(DataSourceRequest.LeafMappingParametersGenerator request) {
        return () -> {
            var mapping = new HashMap<String, Object>();

            mapping.put("store", ESTestCase.randomBoolean());
            mapping.put("index", ESTestCase.randomBoolean());

            if (ESTestCase.randomDouble() <= 0.1) {
                var keywordMultiFieldMapping = keywordMapping(request).get();
                keywordMultiFieldMapping.put("type", "keyword");
                keywordMultiFieldMapping.remove("copy_to");

                mapping.put("fields", Map.of("kwd", keywordMultiFieldMapping));
            }

            return mapping;
        };
    }

    private Supplier<Map<String, Object>> ipMapping() {
        return () -> {
            var mapping = commonMappingParameters();

            if (ESTestCase.randomDouble() <= 0.2) {
                mapping.put("null_value", NetworkAddress.format(ESTestCase.randomIp(ESTestCase.randomBoolean())));
            }

            if (ESTestCase.randomBoolean()) {
                mapping.put("ignore_malformed", ESTestCase.randomBoolean());
            }

            return mapping;
        };
    }

    private Supplier<Map<String, Object>> constantKeywordMapping() {
        return () -> {
            var mapping = new HashMap<String, Object>();

            // value is optional and can be set from the first document
            // we don't cover this case here
            mapping.put("value", ESTestCase.randomAlphaOfLengthBetween(0, 10));

            return mapping;
        };
    }

    private Supplier<Map<String, Object>> wildcardMapping() {
        return () -> {
            var mapping = new HashMap<String, Object>();

            if (ESTestCase.randomDouble() <= 0.2) {
                mapping.put("ignore_above", ESTestCase.randomIntBetween(1, 100));
            }
            if (ESTestCase.randomDouble() <= 0.2) {
                mapping.put("null_value", ESTestCase.randomAlphaOfLengthBetween(0, 10));
            }

            return mapping;
        };
    }

    public static HashMap<String, Object> commonMappingParameters() {
        var map = new HashMap<String, Object>();
        map.put("store", ESTestCase.randomBoolean());
        map.put("index", ESTestCase.randomBoolean());
        map.put("doc_values", ESTestCase.randomBoolean());

        if (ESTestCase.randomBoolean()) {
            map.put(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, ESTestCase.randomFrom("none", "arrays", "all"));
        }

        return map;
    }

    @Override
    public DataSourceResponse.ObjectMappingParametersGenerator handle(DataSourceRequest.ObjectMappingParametersGenerator request) {
        if (request.isNested()) {
            assert request.parentSubobjects() != ObjectMapper.Subobjects.DISABLED;

            return new DataSourceResponse.ObjectMappingParametersGenerator(() -> {
                var parameters = new HashMap<String, Object>();

                if (ESTestCase.randomBoolean()) {
                    parameters.put("dynamic", ESTestCase.randomFrom("true", "false", "strict"));
                }
                if (ESTestCase.randomBoolean()) {
                    parameters.put(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "all");  // [arrays] doesn't apply to nested objects
                }

                return parameters;
            });
        }

        return new DataSourceResponse.ObjectMappingParametersGenerator(() -> {
            var parameters = new HashMap<String, Object>();

            // Changing subobjects from subobjects: false is not supported, but we can f.e. go from "true" to "false".
            // TODO enable subobjects: auto
            // It is disabled because it currently does not have auto flattening and that results in asserts being triggered when using
            // copy_to.
            var subobjects = ESTestCase.randomValueOtherThan(
                ObjectMapper.Subobjects.AUTO,
                () -> ESTestCase.randomFrom(ObjectMapper.Subobjects.values())
            );

            if (request.parentSubobjects() == ObjectMapper.Subobjects.DISABLED || subobjects == ObjectMapper.Subobjects.DISABLED) {
                // "enabled: false" is not compatible with subobjects: false
                // changing "dynamic" from parent context is not compatible with subobjects: false
                // changing subobjects value is not compatible with subobjects: false
                if (ESTestCase.randomBoolean()) {
                    parameters.put("enabled", "true");
                }

                return parameters;
            }

            if (ESTestCase.randomBoolean()) {
                parameters.put("subobjects", subobjects.toString());
            }
            if (ESTestCase.randomBoolean()) {
                parameters.put("dynamic", ESTestCase.randomFrom("true", "false", "strict", "runtime"));
            }
            if (ESTestCase.randomBoolean()) {
                parameters.put("enabled", ESTestCase.randomFrom("true", "false"));
            }

            if (ESTestCase.randomBoolean()) {
                var value = request.isRoot() ? ESTestCase.randomFrom("none", "arrays") : ESTestCase.randomFrom("none", "arrays", "all");
                parameters.put(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, value);
            }

            return parameters;
        });
    }
}
