/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.io.IOException;
import java.util.SortedMap;

/**
 * If field level security is enabled this interceptor disables the request cache for search and shardSearch requests.
 */
public class ShardSearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    private static final ThreadLocal<BytesStreamOutput> threadLocalOutput = ThreadLocal.withInitial(BytesStreamOutput::new);

    public ShardSearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    void disableFeatures(IndicesRequest indicesRequest,
                         SortedMap<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex,
                         ActionListener<Void> listener) {
        final ShardSearchRequest request = (ShardSearchRequest) indicesRequest;
        try {
            BytesReference bytes = serialise(indexAccessControlByIndex);
            request.cacheModifier(bytes);
        } catch (IOException e) {
            listener.onFailure(e);
        }
        listener.onResponse(null);
    }

    private BytesReference serialise(
        SortedMap<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex) throws IOException {
        BytesStreamOutput out = threadLocalOutput.get();
        try {
            for (IndicesAccessControl.IndexAccessControl iac : indexAccessControlByIndex.values()) {
                iac.writeCacheKey(out);
            }
            // copy it over since we don't want to share the thread-local bytes in #scratch
            return out.copyBytes();
        } finally {
            out.reset();
        }
    }

    @Override
    public boolean supports(IndicesRequest request) {
        return request instanceof ShardSearchRequest;
    }
}
