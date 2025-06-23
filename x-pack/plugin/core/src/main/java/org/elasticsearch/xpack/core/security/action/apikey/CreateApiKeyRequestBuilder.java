/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Request builder for populating a {@link CreateApiKeyRequest}
 */
public class CreateApiKeyRequestBuilder extends ActionRequestBuilder<CreateApiKeyRequest, CreateApiKeyResponse> {
    private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder().allowRestriction(true).build();
    private static final ConstructingObjectParser<CreateApiKeyRequest, Void> PARSER = createParser(ROLE_DESCRIPTOR_PARSER::parse);

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<CreateApiKeyRequest, Void> createParser(
        CheckedBiFunction<String, XContentParser, RoleDescriptor, IOException> roleDescriptorParser
    ) {
        ConstructingObjectParser<CreateApiKeyRequest, Void> parser = new ConstructingObjectParser<>(
            "api_key_request",
            false,
            (args, v) -> new CreateApiKeyRequest(
                (String) args[0],
                (List<RoleDescriptor>) args[1],
                TimeValue.parseTimeValue((String) args[2], null, "expiration"),
                (Map<String, Object>) args[3]
            )
        );

        parser.declareString(constructorArg(), new ParseField("name"));
        parser.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> {
            p.nextToken();
            return roleDescriptorParser.apply(n, p);
        }, new ParseField("role_descriptors"));
        parser.declareString(optionalConstructorArg(), new ParseField("expiration"));
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
        return parser;
    }

    public CreateApiKeyRequestBuilder(ElasticsearchClient client) {
        super(client, CreateApiKeyAction.INSTANCE, new CreateApiKeyRequest());
    }

    public CreateApiKeyRequestBuilder setName(String name) {
        request.setName(name);
        return this;
    }

    public CreateApiKeyRequestBuilder setExpiration(TimeValue expiration) {
        request.setExpiration(expiration);
        return this;
    }

    public CreateApiKeyRequestBuilder setRoleDescriptors(List<RoleDescriptor> roleDescriptors) {
        request.setRoleDescriptors(roleDescriptors);
        return this;
    }

    public CreateApiKeyRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        request.setRefreshPolicy(refreshPolicy);
        return this;
    }

    public CreateApiKeyRequestBuilder setMetadata(Map<String, Object> metadata) {
        request.setMetadata(metadata);
        return this;
    }

    public CreateApiKeyRequestBuilder source(BytesReference source, XContentType xContentType) throws IOException {
        CreateApiKeyRequest createApiKeyRequest = parse(source, xContentType);
        setName(createApiKeyRequest.getName());
        setRoleDescriptors(createApiKeyRequest.getRoleDescriptors());
        setExpiration(createApiKeyRequest.getExpiration());
        setMetadata(createApiKeyRequest.getMetadata());
        return this;
    }

    protected CreateApiKeyRequest parse(BytesReference source, XContentType xContentType) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                source,
                xContentType
            )
        ) {
            return parse(parser);
        }
    }

    public static CreateApiKeyRequest parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
