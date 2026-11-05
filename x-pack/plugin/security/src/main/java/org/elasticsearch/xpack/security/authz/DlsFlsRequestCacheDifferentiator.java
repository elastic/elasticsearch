/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.support.SecurityQueryTemplateEvaluator;

import java.io.IOException;
import java.util.TreeSet;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_VALUE;

public class DlsFlsRequestCacheDifferentiator implements CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> {

    private static final Logger logger = LogManager.getLogger(DlsFlsRequestCacheDifferentiator.class);

    private final XPackLicenseState licenseState;
    private final SetOnce<SecurityContext> securityContextHolder;
    private final SetOnce<ScriptService> scriptServiceReference;
    private final Function<ShardId, String> currentIndexName;

    public DlsFlsRequestCacheDifferentiator(
        XPackLicenseState licenseState,
        SetOnce<SecurityContext> securityContextReference,
        SetOnce<ScriptService> scriptServiceReference
    ) {
        this(licenseState, securityContextReference, scriptServiceReference, shardId -> null);
    }

    /**
     * @param currentIndexName the name that the index of a shard has now, or {@code null} if it is not known. A request can carry the
     *                         name the index had before a rename, while the access control of the request holds the names that the data
     *                         node resolved, which are the current ones when the request went through an alias.
     */
    public DlsFlsRequestCacheDifferentiator(
        XPackLicenseState licenseState,
        SetOnce<SecurityContext> securityContextReference,
        SetOnce<ScriptService> scriptServiceReference,
        Function<ShardId, String> currentIndexName
    ) {
        this.currentIndexName = currentIndexName;
        this.licenseState = licenseState;
        this.securityContextHolder = securityContextReference;
        this.scriptServiceReference = scriptServiceReference;
    }

    @Override
    public void accept(ShardSearchRequest request, StreamOutput out) throws IOException {
        final SecurityContext securityContext = securityContextHolder.get();
        final IndicesAccessControl indicesAccessControl = INDICES_PERMISSIONS_VALUE.get(securityContext.getThreadContext());
        final ShardId shardId = request.shardId();
        final String requestedName = shardId.getIndexName();
        // look the index up by the name it has now first, as the reader wrapper does, so that the key describes the restrictions that
        // the reader applies
        final String currentName = currentIndexName.apply(shardId);
        String indexName = requestedName;
        IndicesAccessControl.IndexAccessControl indexAccessControl = null;
        if (currentName != null) {
            indexAccessControl = indicesAccessControl.getIndexPermissions(currentName);
            if (indexAccessControl != null) {
                indexName = currentName;
            }
        }
        if (indexAccessControl == null) {
            indexAccessControl = indicesAccessControl.getIndexPermissions(requestedName);
        }
        if (indexAccessControl == null && currentName != null && currentName.equals(requestedName) == false) {
            // The index was renamed and the access control has an entry under neither name. It is not known which of its entries
            // describe this index, and a key that says nothing could let a result cached for a user without restrictions be served to
            // one with them, so the key stands for all of them.
            for (String name : new TreeSet<>(indicesAccessControl.getIndicesWithFieldOrDocumentLevelSecurity())) {
                writeCacheKey(indicesAccessControl.getIndexPermissions(name), name, securityContext, out);
            }
            return;
        }
        writeCacheKey(indexAccessControl, indexName, securityContext, out);
    }

    private void writeCacheKey(
        IndicesAccessControl.IndexAccessControl indexAccessControl,
        String indexName,
        SecurityContext securityContext,
        StreamOutput out
    ) throws IOException {
        if (indexAccessControl != null
            && (indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()
                || indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions())
            && (indexAccessControl.isDlsFlsImplicit() || DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState))) {
            logger.debug(
                "index [{}] with field level access controls [{}] "
                    + "document level access controls [{}]. Differentiating request cache key",
                indexName,
                indexAccessControl.getFieldPermissions().hasFieldLevelSecurity(),
                indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions()
            );
            indexAccessControl.buildCacheKey(
                out,
                SecurityQueryTemplateEvaluator.wrap(securityContext.getUser(), scriptServiceReference.get())
            );
        }
    }
}
