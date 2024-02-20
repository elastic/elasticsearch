/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class BulkUpdateApiKeyRequest extends BaseBulkUpdateApiKeyRequest {

    public static BulkUpdateApiKeyRequest usingApiKeyIds(String... ids) {
        return new BulkUpdateApiKeyRequest(Arrays.stream(ids).toList(), null, null, null);
    }

    public static BulkUpdateApiKeyRequest wrap(final UpdateApiKeyRequest request) {
        return new BulkUpdateApiKeyRequest(
            List.of(request.getId()),
            request.getRoleDescriptors(),
            request.getMetadata(),
            request.getExpiration()
        );
    }

    public BulkUpdateApiKeyRequest(
        final List<String> ids,
        @Nullable final List<RoleDescriptor> roleDescriptors,
        @Nullable final Map<String, Object> metadata,
        @Nullable final TimeValue expiration
    ) {
        super(ids, roleDescriptors, metadata, expiration);
    }

    public BulkUpdateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ApiKey.Type getType() {
        return ApiKey.Type.REST;
    }

    public interface RequestTranslator {
        BulkUpdateApiKeyRequest translate(RestRequest request) throws IOException;

        class Default implements RequestTranslator {
            private static final ConstructingObjectParser<BulkUpdateApiKeyRequest, Void> PARSER = initParser((n, p) -> {
                try {
                    return RoleDescriptor.parse(n, p, false);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            @SuppressWarnings("unchecked")
            protected static ConstructingObjectParser<BulkUpdateApiKeyRequest, Void> initParser(
                BiFunction<String, XContentParser, RoleDescriptor> roleDescriptorParser
            ) {
                final ConstructingObjectParser<BulkUpdateApiKeyRequest, Void> parser = new ConstructingObjectParser<>(
                    "bulk_update_api_key_request",
                    a -> new BulkUpdateApiKeyRequest(
                        (List<String>) a[0],
                        (List<RoleDescriptor>) a[1],
                        (Map<String, Object>) a[2],
                        TimeValue.parseTimeValue((String) a[3], null, "expiration")
                    )
                );
                parser.declareStringArray(constructorArg(), new ParseField("ids"));
                parser.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> {
                    p.nextToken();
                    return roleDescriptorParser.apply(n, p);
                }, new ParseField("role_descriptors"));
                parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
                parser.declareString(optionalConstructorArg(), new ParseField("expiration"));
                return parser;
            }

            @Override
            public BulkUpdateApiKeyRequest translate(RestRequest request) throws IOException {
                try (XContentParser parser = request.contentParser()) {
                    return PARSER.parse(parser, null);
                }
            }
        }
    }
}
