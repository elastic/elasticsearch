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
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.function.Function;

/**
 * An IndexReader wrapper implementation that is used for field and document level security.
 * <p>
 * Based on the {@link ThreadContext} this class will enable field and/or document level security.
 * <p>
 * Field level security is enabled by wrapping the original {@link DirectoryReader} in a {@link FieldSubsetReader}
 * in the {@link #apply(DirectoryReader)} method.
 * <p>
 * Document level security is enabled by wrapping the original {@link DirectoryReader} in a {@link DocumentSubsetReader}
 * instance.
 */
public class SecurityIndexReaderWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {
    private static final Logger logger = LogManager.getLogger(SecurityIndexReaderWrapper.class);

    private final Function<ShardId, QueryShardContext> queryShardContextProvider;
    private final DocumentSubsetBitsetCache bitsetCache;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final ScriptService scriptService;

    public SecurityIndexReaderWrapper(Function<ShardId, QueryShardContext> queryShardContextProvider,
                                      DocumentSubsetBitsetCache bitsetCache, ThreadContext threadContext, XPackLicenseState licenseState,
                                      ScriptService scriptService) {
        this.scriptService = scriptService;
        this.queryShardContextProvider = queryShardContextProvider;
        this.bitsetCache = bitsetCache;
        this.threadContext = threadContext;
        this.licenseState = licenseState;
    }

    @Override
    public DirectoryReader apply(final DirectoryReader reader) {
        if (licenseState.isDocumentAndFieldLevelSecurityAllowed() == false) {
            return reader;
        }

        try {
            final IndicesAccessControl indicesAccessControl = getIndicesAccessControl();

            ShardId shardId = ShardUtils.extractShardId(reader);
            if (shardId == null) {
                throw new IllegalStateException(LoggerMessageFormat.format("couldn't extract shardId from reader [{}]", reader));
            }

            final IndicesAccessControl.IndexAccessControl permissions = indicesAccessControl.getIndexPermissions(shardId.getIndexName());
            // No permissions have been defined for an index, so don't intercept the index reader for access control
            if (permissions == null) {
                return reader;
            }

            DirectoryReader wrappedReader = reader;
            DocumentPermissions documentPermissions = permissions.getDocumentPermissions();
            if (documentPermissions != null && documentPermissions.hasDocumentLevelPermissions()) {
                BooleanQuery filterQuery = documentPermissions.filter(getUser(), scriptService, shardId, queryShardContextProvider);
                if (filterQuery != null) {
                    wrappedReader = DocumentSubsetReader.wrap(wrappedReader, bitsetCache, new ConstantScoreQuery(filterQuery));
                }
            }

            return permissions.getFieldPermissions().filter(wrappedReader);
        } catch (IOException e) {
            logger.error("Unable to apply field level security");
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    protected IndicesAccessControl getIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl == null) {
            throw Exceptions.authorizationError("no indices permissions found");
        }
        return indicesAccessControl;
    }

    protected User getUser(){
        Authentication authentication = Authentication.getAuthentication(threadContext);
        return authentication.getUser();
    }

}
