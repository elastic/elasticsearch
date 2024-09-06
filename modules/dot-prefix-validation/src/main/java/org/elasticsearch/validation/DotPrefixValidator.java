/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;

import java.util.Optional;
import java.util.Set;

public abstract class DotPrefixValidator<RequestType> implements MappedActionFilter {
    public static final Setting<Boolean> VALIDATE_DOT_PREFIXES = Setting.boolSetting(
        "cluster.indices.validate_dot_prefixes",
        true,
        Setting.Property.NodeScope
    );
    DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DotPrefixValidator.class);

    private final ThreadContext threadContext;
    private final boolean isEnabled;

    public DotPrefixValidator(ThreadContext threadContext, ClusterService clusterService) {
        this.threadContext = threadContext;
        this.isEnabled = VALIDATE_DOT_PREFIXES.get(clusterService.getSettings());
    }

    protected abstract Set<String> getIndicesFromRequest(RequestType request);

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        Set<String> indices = getIndicesFromRequest((RequestType) request);
        if (isEnabled) {
            validateIndices(indices);
        }
        chain.proceed(task, action, request, listener);
    }

    void validateIndices(@Nullable Set<String> indices) {
        if (indices != null && isInternalRequest() == false) {
            for (String index : indices) {
                if (Strings.hasLength(index)) {
                    char c = getFirstChar(index);
                    if (c == '.') {
                        deprecationLogger.warn(
                            DeprecationCategory.INDICES,
                            "dot-prefix",
                            "Index [{}] name begins with a dot (.), which is deprecated, "
                                + "and will not be allowed in a future Elasticsearch version.",
                            index
                        );
                    }
                }
            }
        }
    }

    private static char getFirstChar(String index) {
        char c = index.charAt(0);
        if (c == '<') {
            // Date-math is being used for the index, we need to
            // consider it by stripping the first '<' before we
            // check for a dot-prefix
            String strippedLeading = index.substring(1);
            if (Strings.hasLength(strippedLeading)) {
                c = strippedLeading.charAt(0);
            }
        }
        return c;
    }

    private boolean isInternalRequest() {
        final String actionOrigin = threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME);
        final boolean isSystemContext = threadContext.isSystemContext();
        final boolean isInternalOrigin = Optional.ofNullable(actionOrigin).map(Strings::hasText).orElse(false);
        final boolean hasElasticOriginHeader = Optional.ofNullable(threadContext.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER))
            .map(Strings::hasText)
            .orElse(false);
        return isSystemContext || isInternalOrigin || hasElasticOriginHeader;
    }
}
