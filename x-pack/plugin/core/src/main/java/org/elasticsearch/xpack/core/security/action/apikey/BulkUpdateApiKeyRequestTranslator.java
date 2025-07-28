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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public interface BulkUpdateApiKeyRequestTranslator {
    BulkUpdateApiKeyRequest translate(RestRequest request) throws IOException;

    class Default implements BulkUpdateApiKeyRequestTranslator {
        private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder().allowRestriction(true).build();
        private static final ConstructingObjectParser<BulkUpdateApiKeyRequest, Void> PARSER = createParser(
            (n, p) -> ROLE_DESCRIPTOR_PARSER.parse(n, p)
        );

        @SuppressWarnings("unchecked")
        protected static ConstructingObjectParser<BulkUpdateApiKeyRequest, Void> createParser(
            CheckedBiFunction<String, XContentParser, RoleDescriptor, IOException> roleDescriptorParser
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
