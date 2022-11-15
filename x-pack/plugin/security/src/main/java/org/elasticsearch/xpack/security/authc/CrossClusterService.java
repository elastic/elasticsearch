/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CrossClusterService {
    private static final Logger LOGGER = LogManager.getLogger(CrossClusterService.class);

    private final ApiKeyService apiKeyService;

    public CrossClusterService(final ApiKeyService apiKeyService) {
        this.apiKeyService = apiKeyService;
    }

    void validateCrossClusterCredentials(
        ThreadContext ctx,
        ApiKeyService.ApiKeyCredentials fcApiKeyCredentials,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        this.apiKeyService.loadApiKeyAndValidateCredentials(ctx, fcApiKeyCredentials, listener);
    }

    public List<RoleDescriptor> parseRoleDescriptorsBytes(BytesReference bytesReference) {
        if (bytesReference == null) {
            return Collections.emptyList();
        }
        final List<RoleDescriptor> roleDescriptors = new ArrayList<>();
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, bytesReference, XContentType.JSON)) {
            parser.nextToken(); // skip outer start object
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                parser.nextToken(); // role name
                String roleName = parser.currentName();
                roleDescriptors.add(RoleDescriptor.parse(roleName, parser, false));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return roleDescriptors;
    }
}
