/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;

import java.util.Objects;

public class CCMService {

    private final CCMPersistentStorageService ccmPersistentStorageService;
    private final AuthorizationTaskExecutor authorizationTaskExecutor;

    public CCMService(CCMPersistentStorageService ccmPersistentStorageService, AuthorizationTaskExecutor authTaskExecutor) {
        this.ccmPersistentStorageService = Objects.requireNonNull(ccmPersistentStorageService);
        this.authorizationTaskExecutor = Objects.requireNonNull(authTaskExecutor);
        // TODO initialize the cache for the CCM configuration
    }

    public void isEnabled(ActionListener<Boolean> listener) {
        // TODO use cache or cluster state to determine if CCM is enabled
        var ccmModelListener = ActionListener.<CCMModel>wrap(ignored -> listener.onResponse(true), e -> {
            if (e instanceof ResourceNotFoundException) {
                listener.onResponse(false);
                return;
            }

            listener.onFailure(e);
        });

        ccmPersistentStorageService.get(ccmModelListener);
    }

    public void storeConfiguration(CCMModel model, ActionListener<Void> listener) {
        // TODO invalidate the cache
        ccmPersistentStorageService.store(model, listener.delegateFailureIgnoreResponseAndWrap(delegate -> {
            authorizationTaskExecutor.init();
            delegate.onResponse(null);
        }));
    }

    public void getConfiguration(ActionListener<CCMModel> listener) {
        // TODO get this from the cache instead
        ccmPersistentStorageService.get(listener);
    }

    public void disableCCM(ActionListener<Void> listener) {
        authorizationTaskExecutor.shutdown();
        ccmPersistentStorageService.delete(listener);
        // TODO implement invalidating the cache
    }
}
