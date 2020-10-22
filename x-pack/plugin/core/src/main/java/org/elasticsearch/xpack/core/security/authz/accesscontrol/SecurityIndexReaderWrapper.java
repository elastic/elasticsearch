/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.ReaderWrapperFactory;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * An IndexReader wrapper implementation that is used for field and document level security.
 * <p>
 * Based on the {@link ThreadContext} this class will enable field and/or document level security.
 * <p>
 * Field level security is enabled by wrapping the original {@link DirectoryReader} in a {@link FieldSubsetReader}
 * in the {@link #getWrapper(ShardId)} (DirectoryReader)} method.
 * <p>
 * Document level security is enabled by wrapping the original {@link DirectoryReader} in a {@link DocumentSubsetReader}
 * instance.
 */
public class SecurityIndexReaderWrapper implements ReaderWrapperFactory {
    private static final Logger logger = LogManager.getLogger(SecurityIndexReaderWrapper.class);

    private final Function<ShardId, QueryShardContext> queryShardContextProvider;
    private final DocumentSubsetBitsetCache bitsetCache;
    private final XPackLicenseState licenseState;
    private final SecurityContext securityContext;
    private final ScriptService scriptService;

    public SecurityIndexReaderWrapper(Function<ShardId, QueryShardContext> queryShardContextProvider,
                                      DocumentSubsetBitsetCache bitsetCache, SecurityContext securityContext,
                                      XPackLicenseState licenseState, ScriptService scriptService) {
        this.scriptService = scriptService;
        this.queryShardContextProvider = queryShardContextProvider;
        this.bitsetCache = bitsetCache;
        this.securityContext = securityContext;
        this.licenseState = licenseState;
    }

    @Override
    public CheckedFunction<DirectoryReader, DirectoryReader, IOException> getWrapper(ShardId shardId) {
        if (licenseState.isSecurityEnabled() == false ||
            licenseState.checkFeature(Feature.SECURITY_DLS_FLS) == false) {
            return null;
        }
        final Thread createThread = Thread.currentThread();
        final IndicesAccessControl indicesAccessControl = getIndicesAccessControl();
        final IndicesAccessControl.IndexAccessControl permissions = indicesAccessControl.getIndexPermissions(shardId.getIndexName());
        // No permissions have been defined for an index, so don't intercept the index reader for access control
        if (permissions == null) {
            return null;
        }
        final DocumentPermissions documentPermissions = permissions.getDocumentPermissions();
        final BooleanQuery filterQuery;
        if (documentPermissions != null && documentPermissions.hasDocumentLevelPermissions()) {
            try {
                filterQuery = documentPermissions.filter(getUser(), scriptService, shardId, queryShardContextProvider);
            } catch (IOException e) {
                logger.error("Unable to retrieve document level filter");
                throw ExceptionsHelper.convertToElastic(e);
            }
        } else {
            filterQuery = null;
        }
        if (filterQuery == null && permissions.getFieldPermissions().hasFieldLevelSecurity() == false) {
            return null;
        }
        return reader -> {
            try {
                final ShardId readerShardId = ShardUtils.extractShardId(reader);
                if (shardId.equals(readerShardId) == false) {
                    assert false : readerShardId + " != " + shardId;
                    throw new IllegalArgumentException("Wrapping wrong reader; expected shardId " + shardId + "; but " + readerShardId);
                }
                if (Thread.currentThread() != createThread) {
                    assert false : Thread.currentThread() + " != " + createThread;
                    throw new IllegalStateException("Wrapping reader on a different thread");
                }
                DirectoryReader wrappedReader = reader;
                if (filterQuery != null) {
                    wrappedReader = DocumentSubsetReader.wrap(wrappedReader, bitsetCache, new ConstantScoreQuery(filterQuery));
                }
                return permissions.getFieldPermissions().filter(wrappedReader);
            } catch (IOException e) {
                logger.error("Unable to apply field level security");
                throw ExceptionsHelper.convertToElastic(e);
            }
        };
    }

    protected IndicesAccessControl getIndicesAccessControl() {
        final ThreadContext threadContext = securityContext.getThreadContext();
        IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl == null) {
            throw Exceptions.authorizationError("no indices permissions found");
        }
        return indicesAccessControl;
    }

    protected User getUser() {
        return Objects.requireNonNull(securityContext.getUser());
    }

}
