/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parser for a {@link ClientYamlSuiteRestApi}.
 */
public class ClientYamlSuiteRestApiParser {

    private static final ObjectParser<Parameter, Void> PARAMETER_PARSER = new ObjectParser<>("parameter", true, Parameter::new);
    static {
        PARAMETER_PARSER.declareBoolean(Parameter::setRequired, new ParseField("required"));
    }

    public ClientYamlSuiteRestApi parse(String location, XContentParser parser) throws IOException {

        while (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            // move to first field name
        }

        String apiName = parser.currentName();
        if (location.endsWith(apiName + ".json") == false) {
            throw new IllegalArgumentException("API [" + apiName + "] should have the same name as its file [" + location + "]");
        }
        if (apiName.chars().filter(c -> c == '.').count() > 1) {
            throw new IllegalArgumentException("API [" + apiName + "] contains more then one namespace [" + location + "]");
        }

        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApi(location, apiName);

        int level = -1;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT || level >= 0) {

            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                if ("documentation".equals(parser.currentName())) {
                    parser.nextToken();
                    parser.skipChildren();
                } else if ("headers".equals(parser.currentName())) {
                    assert parser.nextToken() == XContentParser.Token.START_OBJECT;
                    String headerName = null;
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                            headerName = parser.currentName();
                        }
                        if (headerName.equals("accept")) {
                            if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                                throw new ParsingException(parser.getTokenLocation(), apiName + " API: [headers.accept] must be an array");
                            }
                            List<String> acceptMimeTypes = getStringsFromArray(parser, "accept");
                            restApi.setResponseMimeTypes(acceptMimeTypes);
                        } else if (headerName.equals("content_type")) {
                            if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    apiName + " API: [headers.content_type] must be an array"
                                );
                            }
                            List<String> requestMimeTypes = getStringsFromArray(parser, "content_type");
                            restApi.setRequestMimeTypes(requestMimeTypes);
                        }
                    }
                } else if ("stability".equals(parser.currentName())) {
                    parser.nextToken();
                    restApi.setStability(parser.textOrNull());
                } else if ("visibility".equals(parser.currentName())) {
                    parser.nextToken();
                    restApi.setVisibility(parser.textOrNull());
                } else if ("feature_flag".equals(parser.currentName())) {
                    parser.nextToken();
                    restApi.setFeatureFlag(parser.textOrNull());
                } else if ("deprecated".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            apiName + " API: expected [deprecated] field in rest api definition to hold an object"
                        );
                    }
                    parser.skipChildren();
                } else if ("url".equals(parser.currentName())) {
                    String currentFieldName = null;
                    assert parser.nextToken() == XContentParser.Token.START_OBJECT;

                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        }
                        if ("paths".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                                throw new ParsingException(parser.getTokenLocation(), apiName + " API: [paths] must be an array");
                            }
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                String path = null;
                                Set<String> methods = new HashSet<>();
                                Set<String> pathParts = new HashSet<>();
                                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                    if ("path".equals(parser.currentName())) {
                                        parser.nextToken();
                                        path = parser.text();
                                    } else if ("methods".equals(parser.currentName())) {
                                        if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                                            throw new ParsingException(
                                                parser.getTokenLocation(),
                                                apiName + " API: expected [methods] field in rest api definition to hold an array"
                                            );
                                        }
                                        while (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
                                            String method = parser.text();
                                            if (methods.add(method) == false) {
                                                throw new ParsingException(
                                                    parser.getTokenLocation(),
                                                    apiName + " API: found duplicate method [" + method + "]"
                                                );
                                            }
                                        }
                                    } else if ("parts".equals(parser.currentName())) {
                                        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                            throw new ParsingException(
                                                parser.getTokenLocation(),
                                                apiName + " API: expected [parts] field in rest api definition to hold an object"
                                            );
                                        }
                                        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                                            String part = parser.currentName();
                                            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                                throw new ParsingException(
                                                    parser.getTokenLocation(),
                                                    apiName + " API: expected [parts] field in rest api definition to contain an object"
                                                );
                                            }
                                            parser.skipChildren();
                                            if (pathParts.add(part) == false) {
                                                throw new ParsingException(
                                                    parser.getTokenLocation(),
                                                    apiName + " API: duplicated path part [" + part + "]"
                                                );
                                            }
                                        }
                                    } else if ("deprecated".equals(parser.currentName())) {
                                        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                            throw new ParsingException(
                                                parser.getTokenLocation(),
                                                apiName + " API: expected [deprecated] field in rest api definition to hold an object"
                                            );
                                        }
                                        parser.skipChildren();
                                    } else {
                                        throw new ParsingException(
                                            parser.getTokenLocation(),
                                            apiName
                                                + " API: unexpected field ["
                                                + parser.currentName()
                                                + "] of type ["
                                                + parser.currentToken()
                                                + "]"
                                        );
                                    }
                                }
                                restApi.addPath(path, methods.toArray(new String[0]), pathParts);
                            }
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                apiName + " API: unsupported field [" + parser.currentName() + "]"
                            );
                        }
                    }
                } else if ("params".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            apiName + " API: expected [params] field in rest api definition to contain an object"
                        );

                    }
                    while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String param = parser.currentName();
                        parser.nextToken();
                        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                apiName + " API: expected [params] field in rest api definition to contain an object"
                            );
                        }
                        restApi.addParam(param, PARAMETER_PARSER.parse(parser, null).isRequired());
                    }
                } else if ("body".equals(parser.currentName())) {
                    parser.nextToken();
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        boolean requiredFound = false;
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                                if ("required".equals(parser.currentName())) {
                                    requiredFound = true;
                                    parser.nextToken();
                                    if (parser.booleanValue()) {
                                        restApi.setBodyRequired();
                                    } else {
                                        restApi.setBodyOptional();
                                    }
                                }
                            }
                        }
                        if (false == requiredFound) {
                            restApi.setBodyOptional();
                        }
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        apiName + " API: unsupported field [" + parser.currentName() + "]"
                    );
                }
            }

            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                level++;
            }
            if (parser.currentToken() == XContentParser.Token.END_OBJECT) {
                level--;
            }
        }

        parser.nextToken();
        assert parser.currentToken() == XContentParser.Token.END_OBJECT : "Expected [END_OBJECT] but was [" + parser.currentToken() + "]";
        parser.nextToken();

        if (restApi.getPaths().isEmpty()) {
            throw new IllegalArgumentException(apiName + " API: at least one path should be listed under [paths]");
        }
        if (restApi.getStability() == null) {
            throw new IllegalArgumentException(apiName + " API does not declare its stability in [" + location + "]");
        }
        if (restApi.getVisibility() == null) {
            throw new IllegalArgumentException(apiName + " API does not declare its visibility explicitly in [" + location + "]");
        }
        if (restApi.getVisibility() == ClientYamlSuiteRestApi.Visibility.FEATURE_FLAG
            && (restApi.getFeatureFlag() == null || restApi.getFeatureFlag().isEmpty())) {
            throw new IllegalArgumentException(
                apiName + " API has visibility `feature_flag` but does not document its feature flag in [" + location + "]"
            );
        }
        if (restApi.getFeatureFlag() != null && restApi.getVisibility() != ClientYamlSuiteRestApi.Visibility.FEATURE_FLAG) {
            throw new IllegalArgumentException(
                apiName + " API does not have visibility `feature_flag` but documents a feature flag [" + location + "]"
            );
        }
        return restApi;
    }

    private List<String> getStringsFromArray(XContentParser parser, String key) throws IOException {
        return parser.list().stream().filter(Objects::nonNull).map(o -> {
            if (o instanceof String) {
                return (String) o;
            } else {
                throw new XContentParseException(
                    key + " array may only contain strings but found [" + o.getClass().getName() + "] [" + o + "]"
                );
            }
        }).collect(Collectors.toList());
    }

    private static class Parameter {
        private boolean required;

        public boolean isRequired() {
            return required;
        }

        public void setRequired(boolean required) {
            this.required = required;
        }
    }
}
