/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public interface UpdateApiKeyRequestTranslator {
    UpdateApiKeyRequest translate(RestRequest request) throws IOException;

    class Default implements UpdateApiKeyRequestTranslator {
        private static final ConstructingObjectParser<Payload, Void> PARSER = createParser((n, p) -> RoleDescriptor.parse(n, p, false));

        @SuppressWarnings("unchecked")
        protected static ConstructingObjectParser<Payload, Void> createParser(
            CheckedBiFunction<String, XContentParser, RoleDescriptor, IOException> roleDescriptorParser
        ) {
            final ConstructingObjectParser<Payload, Void> parser = new ConstructingObjectParser<>(
                "update_api_key_request_payload",
                a -> new Payload(
                    (List<RoleDescriptor>) a[0],
                    (Map<String, Object>) a[1],
                    TimeValue.parseTimeValue((String) a[2], null, "expiration")
                )
            );
            parser.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> {
                p.nextToken();
                return roleDescriptorParser.apply(n, p);
            }, new ParseField("role_descriptors"));
            parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
            parser.declareString(optionalConstructorArg(), new ParseField("expiration"));
            return parser;
        }

        @Override
        public UpdateApiKeyRequest translate(RestRequest request) throws IOException {
            // Note that we use `ids` here even though we only support a single ID. This is because the route where this translator is used
            // shares a path prefix with `RestClearApiKeyCacheAction` and our current REST implementation requires that path params have the
            // same wildcard if their paths share a prefix
            final String apiKeyId = request.param("ids");
            if (false == request.hasContent()) {
                return UpdateApiKeyRequest.usingApiKeyId(apiKeyId);
            }
            final Payload payload = PARSER.parse(request.contentParser(), null);
            return new UpdateApiKeyRequest(apiKeyId, payload.roleDescriptors, payload.metadata, payload.expiration);
        }

        protected record Payload(List<RoleDescriptor> roleDescriptors, Map<String, Object> metadata, TimeValue expiration) {}
    }
}
