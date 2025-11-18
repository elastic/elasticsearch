/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;

import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_CCM_PATH;

/**
 * Returns a class to handle modifying the HTTP requests with the appropriate CCM authentication information if CCM is configured.
 */
public class CCMAuthenticationApplierFactory {

    public static final NoopApplier NOOP_APPLIER = new NoopApplier();

    private final CCMFeature ccmFeature;
    private final CCMService ccmService;

    public CCMAuthenticationApplierFactory(CCMFeature ccmFeature, CCMService ccmService) {
        this.ccmFeature = Objects.requireNonNull(ccmFeature);
        this.ccmService = Objects.requireNonNull(ccmService);
    }

    public interface AuthApplier extends Function<HttpRequestBase, HttpRequestBase> {}

    public void getAuthenticationApplier(ActionListener<AuthApplier> listener) {
        if (ccmFeature.isCcmSupportedEnvironment() == false) {
            listener.onResponse(NOOP_APPLIER);
            return;
        }

        SubscribableListener.newForked(ccmService::isEnabled).<CCMModel>andThen((ccmModelListener, enabled) -> {
            if (enabled == null || enabled == false) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Cloud connected mode is not configured, please configure it using PUT {} "
                            + "before accessing the Elastic Inference Service.",
                        RestStatus.BAD_REQUEST,
                        INFERENCE_CCM_PATH
                    )
                );
                return;
            }

            ccmService.getConfiguration(ccmModelListener);
        }).<AuthApplier>andThenApply(ccmModel -> new AuthenticationHeaderApplier(ccmModel.apiKey())).addListener(listener);
    }

    /**
     * If CCM is configured and enabled this class will apply the appropriate authentication header to the request.
     */
    public record AuthenticationHeaderApplier(SecureString apiKey) implements AuthApplier {
        public AuthenticationHeaderApplier(String apiKey) {
            this(new SecureString(Objects.requireNonNull(apiKey).toCharArray()));
        }

        @Override
        public HttpRequestBase apply(HttpRequestBase request) {
            request.setHeader(createAuthBearerHeader(apiKey));
            return request;
        }
    }

    /**
     * If CCM is not configured this class will not modify the request because no authentication is necessary since mTLS certs are used.
     */
    public record NoopApplier() implements AuthApplier {
        @Override
        public HttpRequestBase apply(HttpRequestBase request) {
            return request;
        }
    }

}
