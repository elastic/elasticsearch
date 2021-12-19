/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;

public class ProfileService {
    private static final Logger logger = LogManager.getLogger(ProfileService.class);
    private static final String DOC_ID_PREFIX = "profile_";

    private final Settings settings;
    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager profileIndex;
    private final ThreadPool threadPool;

    public ProfileService(Settings settings, Clock clock, Client client, SecurityIndexManager profileIndex, ThreadPool threadPool) {
        this.settings = settings;
        this.clock = clock;
        this.client = client;
        this.profileIndex = profileIndex;
        this.threadPool = threadPool;
    }

    public void getProfile(String uid, @Nullable Set<String> dataKeys, ActionListener<Profile> listener) {
        if (maybeHandleIndexStatusIssue(listener)) {
            return;
        }

        final GetRequest getRequest = new GetRequest(SECURITY_PROFILE_ALIAS, uidToDocId(uid));
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
            profileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(client, SECURITY_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(response -> {
                    if (false == response.isExists()) {
                        logger.debug("profile with uid [{}] does not exist", uid);
                        listener.onResponse(null);
                        return;
                    }
                    listener.onResponse(
                        buildProfile(response.getSourceAsBytesRef(), response.getPrimaryTerm(), response.getSeqNo(), dataKeys)
                    );
                }, listener::onFailure))
            );
        }
    }

    public void getProfile(Authentication authentication, ActionListener<Profile> listener) {
        if (maybeHandleIndexStatusIssue(listener)) {
            return;
        }

        final SearchRequest searchRequest = client.prepareSearch(SECURITY_PROFILE_ALIAS)
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("user.username", authentication.getUser().principal()))
                    // TODO: this will be replaced by domain lookup and reverse lookup
                    .must(QueryBuilders.termQuery("user.realm.name", authentication.getSourceRealm().getName()))
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
                        final SearchHit[] hits = searchHits.getHits();
                        if (hits.length < 1) {
                            logger.debug(
                                "profile does not exist for username [{}] and realm name [{}]",
                                authentication.getUser().principal(),
                                authentication.getSourceRealm().getName()
                            );
                            listener.onResponse(null);
                        } else if (hits.length == 1) {
                            final SearchHit hit = hits[0];
                            listener.onResponse(buildProfile(hit.getSourceRef(), hit.getPrimaryTerm(), hit.getSeqNo(), Set.of()));
                        } else {
                            final ParameterizedMessage errorMessage = new ParameterizedMessage(
                                "multiple [{}] profiles [{}] found for user [{}]",
                                hits.length,
                                Arrays.stream(hits).map(SearchHit::getId).map(this::docIdToUid).sorted().collect(Collectors.joining(",")),
                                // TODO: include domain information
                                authentication.getUser().principal()
                            );
                            logger.error(errorMessage);
                            listener.onFailure(new IllegalStateException(errorMessage.getFormattedMessage()));
                        }
                    }, listener::onFailure)
                )
            );
        }
    }

    private String uidToDocId(String uid) {
        return DOC_ID_PREFIX + uid;
    }

    private String docIdToUid(String docId) {
        if (docId == null || false == docId.startsWith(DOC_ID_PREFIX)) {
            throw new IllegalStateException("profile document ID [" + docId + "] has unexpected value");
        }
        return docId.substring(DOC_ID_PREFIX.length());
    }

    private Profile buildProfile(BytesReference source, long primaryTerm, long seqNo, Set<String> dataKeys) throws IOException {
        if (source == null) {
            throw new IllegalStateException("profile document did not have source but source should have been fetched");
        }
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, XContentType.JSON)) {
            // TODO: replace null with actual domain lookup
            return ProfileDocument.fromXContent(parser).toProfile(primaryTerm, seqNo, null, dataKeys);
        }
    }

    private <T> boolean maybeHandleIndexStatusIssue(ActionListener<T> listener) {
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
}
