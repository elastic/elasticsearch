/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SecurityProfileService {

    private static final Logger logger = LogManager.getLogger(SecurityProfileService.class);

    private final Settings settings;
    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager profileIndex;
    private final CompositeRolesStore rolesStore;
    private final ThreadPool threadPool;

    public SecurityProfileService(
        Settings settings,
        Clock clock,
        Client client,
        SecurityIndexManager profileIndex,
        CompositeRolesStore rolesStore,
        ThreadPool threadPool
    ) {
        this.settings = settings;
        this.clock = clock;
        this.client = client;
        this.profileIndex = profileIndex;
        this.rolesStore = rolesStore;
        this.threadPool = threadPool;
    }

    public void getProfileByUid(String uid, ActionListener<Profile> listener) {
        if (maybeHandleIndexStatusIssue(listener)) {
            return;
        }

        final GetRequest getRequest = new GetRequest(SecurityProfileIndex.SECURITY_PROFILE_INDEX_ALIAS, getDocId(uid));
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
            profileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(client, SECURITY_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(response -> {
                    if (false == response.isExists()) {
                        listener.onResponse(null);
                        return;
                    }
                    listener.onResponse(Profile.fromSource(response.getSource(), response.getPrimaryTerm(), response.getSeqNo()));
                }, listener::onFailure))
            );
        }
    }

    public void getProfileByQualifiedName(Profile.QualifiedName qualifiedName, ActionListener<Profile> listener) {
        if (maybeHandleIndexStatusIssue(listener)) {
            return;
        }

        final SearchRequest searchRequest = client.prepareSearch(SecurityProfileIndex.SECURITY_PROFILE_INDEX_ALIAS)
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("user.username", qualifiedName.getUsername()))
                    .must(QueryBuilders.termQuery("user.domain", qualifiedName.getDomain()))
            )
            .request();
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
            profileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    SearchAction.INSTANCE,
                    searchRequest,
                    ActionListener.wrap(searchResponse -> {
                        final SearchHits searchHits = searchResponse.getHits();
                        if (searchHits.getHits().length < 1) {
                            listener.onResponse(null);
                            return;
                        } else if (searchHits.getHits().length == 1) {
                            final SearchHit hits = searchHits.getAt(0);
                            listener.onResponse(Profile.fromSource(hits.getSourceAsMap(), hits.getPrimaryTerm(), hits.getSeqNo()));
                        } else {
                            listener.onFailure(
                                new IllegalStateException("multiple profile found for the same qualified name [" + qualifiedName + "]")
                            );
                        }
                    }, listener::onFailure)
                )
            );
        }
    }

    public void syncProfile(Authentication authentication, ActionListener<AcknowledgedResponse> listener) {
        final User user = authentication.getUser();
        final Authentication.RealmRef realm = authentication.getSourceRealm();
        final Profile.QualifiedName qualifiedName = new Profile.QualifiedName(user.principal(), realm.getEffectiveDomain());

        final Set<String> roles = new HashSet<>(Arrays.asList(user.roles()));
        // TODO: anonymous roles
        rolesStore.getRoleDescriptors(roles, ActionListener.wrap(roleDescriptors -> {
            final List<String> kibanaSpaces = collectKibanaSpaces(roleDescriptors);
            getProfileByQualifiedName(qualifiedName, ActionListener.wrap(profile -> {
                final boolean newProfile = profile == null;
                final String uid = newProfile ? UUIDs.base64UUID() : profile.getUid();

                XContentBuilder builder = profileDoc(user, realm, kibanaSpaces, uid);

                final IndexRequest indexRequest;
                if (newProfile) {
                    indexRequest = client.prepareIndex(SecurityProfileIndex.SECURITY_PROFILE_INDEX_ALIAS)
                        .setSource(builder)
                        .setId(getDocId(uid))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                        .setOpType(DocWriteRequest.OpType.CREATE)
                        .request();

                } else {
                    indexRequest = client.prepareIndex(SecurityProfileIndex.SECURITY_PROFILE_INDEX_ALIAS)
                        .setSource(builder)
                        .setId(getDocId(uid))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                        .setOpType(DocWriteRequest.OpType.UPDATE)
                        .setIfPrimaryTerm(profile.getVersionControl().getPrimaryTerm())
                        .setIfSeqNo(profile.getVersionControl().getSeqNo())
                        .request();
                }

                profileIndex.prepareIndexIfNeededThenExecute(
                    listener::onFailure,
                    () -> executeAsyncWithOrigin(
                        client,
                        SECURITY_ORIGIN,
                        BulkAction.INSTANCE,
                        TransportSingleItemBulkWriteAction.toSingleItemBulkRequest(indexRequest),
                        TransportSingleItemBulkWriteAction.<IndexResponse>wrapBulkResponse(ActionListener.wrap(indexResponse -> {
                            assert getDocId(uid).equals(indexResponse.getId());
                            listener.onResponse(AcknowledgedResponse.TRUE);
                        }, listener::onFailure))
                    )
                );

            }, listener::onFailure));
        }, listener::onFailure));
    }

    private List<String> collectKibanaSpaces(Set<RoleDescriptor> roleDescriptors) {
        final List<String> kibanaSpaces = new ArrayList<>();
        for (RoleDescriptor roleDescriptor : roleDescriptors) {
            for (RoleDescriptor.ApplicationResourcePrivileges applicationPrivilege : roleDescriptor.getApplicationPrivileges()) {
                if (applicationPrivilege.getApplication().startsWith("kibana-")) {
                    for (String resource : applicationPrivilege.getResources()) {
                        if (resource.startsWith("space:")) {
                            kibanaSpaces.add(resource.substring(6));
                        }
                    }
                }
            }
        }
        return kibanaSpaces;
    }

    private XContentBuilder profileDoc(User user, Authentication.RealmRef realm, List<String> kibanaSpaces, String uid) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("uid", uid).field("enabled", user.enabled()).field("last_synchronized", clock.millis());
            builder.startObject("user");
            {
                builder.field("username", user.principal())
                    .field("email", user.email())
                    .field("full_name", user.fullName())
                    .field("display_name", user.fullName());
                builder.startObject("realm");
                {
                    builder.field("name", realm.getName()).field("type", realm.getType()).field("domain", realm.getEffectiveDomain());
                }
                builder.endObject();
            }
            builder.endObject();

            builder.startObject("access");
            {
                builder.field("roles", user.roles());
                builder.startObject("kibana");
                {
                    builder.field("spaces", kibanaSpaces);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private boolean maybeHandleIndexStatusIssue(ActionListener<Profile> listener) {
        final SecurityIndexManager frozenProfileIndex = profileIndex.freeze();
        if (false == frozenProfileIndex.indexExists()) {
            logger.debug("profile index does not exist");
            listener.onResponse(null);
            return true;
        } else if (false == frozenProfileIndex.isAvailable()) {
            listener.onFailure(frozenProfileIndex.getUnavailableReason());
            return true;
        }
        return false;
    }

    private String getDocId(String uid) {
        return "uid_" + uid;
    }
}
