/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.stream.Collectors.toList;

public class TransformDeprecationChecker implements DeprecationChecker {

    public static final String TRANSFORM_DEPRECATION_KEY = "transform_settings";

    private static final Logger logger = LogManager.getLogger(TransformDeprecationChecker.class);

    @Override
    public boolean enabled(Settings settings) {
        // always enabled
        return true;
    }

    @Override
    public void check(Components components, ActionListener<CheckResult> deprecationIssueListener) {

        PageParams startPage = new PageParams(0, PageParams.DEFAULT_SIZE);
        Collection<DeprecationIssue> issues = new ConcurrentLinkedQueue<>();
        recursiveGetTransformsAndCollectDeprecations(
            components,
            issues,
            startPage,
            ActionListener.wrap(
                allIssues -> { deprecationIssueListener.onResponse(new CheckResult(getName(), allIssues)); },
                deprecationIssueListener::onFailure
            )
        );
    }

    @Override
    public String getName() {
        return TRANSFORM_DEPRECATION_KEY;
    }

    private void recursiveGetTransformsAndCollectDeprecations(
        Components components,
        Collection<DeprecationIssue> issues,
        PageParams page,
        ActionListener<List<DeprecationIssue>> listener
    ) {
        final GetTransformAction.Request request = new GetTransformAction.Request(Metadata.ALL);
        request.setPageParams(page);
        request.setAllowNoResources(true);

        ClientHelper.executeAsyncWithOrigin(
            components.client(),
            ClientHelper.DEPRECATION_ORIGIN,
            GetTransformAction.INSTANCE,
            request,
            ActionListener.wrap(getTransformResponse -> {
                final int numberOfTransforms = getTransformResponse.getTransformConfigurations().size();
                Runnable processNextPage = () -> {
                    if (getTransformResponse.getCount() >= (page.getFrom() + page.getSize())) {
                        PageParams nextPage = new PageParams(page.getFrom() + page.getSize(), PageParams.DEFAULT_SIZE);
                        recursiveGetTransformsAndCollectDeprecations(components, issues, nextPage, listener);
                    } else {
                        listener.onResponse(new ArrayList<>(issues));
                    }
                };
                if (numberOfTransforms == 0) {
                    processNextPage.run();
                    return;
                }
                final CountDown numberOfResponsesToProcess = new CountDown(numberOfTransforms);
                for (TransformConfig config : getTransformResponse.getTransformConfigurations()) {
                    issues.addAll(config.checkForDeprecations(components.xContentRegistry()));

                    ValidateTransformAction.Request validateTransformRequest = new ValidateTransformAction.Request(
                        config,
                        false,
                        TimeValue.timeValueSeconds(30)
                    );
                    ActionListener<ValidateTransformAction.Response> validateTransformListener = ActionListener.wrap(
                        validateTransformResponse -> {
                            List<String> warningHeaders = components.client()
                                .threadPool()
                                .getThreadContext()
                                .getResponseHeaders()
                                .get("Warning");
                            if (warningHeaders != null) {
                                issues.addAll(
                                    warningHeaders.stream()
                                        .map(warningHeader -> createDeprecationIssue(config.getId(), warningHeader))
                                        .collect(toList())
                                );
                            }
                            if (numberOfResponsesToProcess.countDown()) {
                                processNextPage.run();
                            }
                        },
                        e -> {
                            logger.warn("An exception occurred while gathering deprecation warnings for transform", e);
                            if (numberOfResponsesToProcess.countDown()) {
                                processNextPage.run();
                            }
                        }
                    );

                    components.client().execute(ValidateTransformAction.INSTANCE, validateTransformRequest, validateTransformListener);
                }
            }, listener::onFailure)
        );
    }

    private static DeprecationIssue createDeprecationIssue(String transformId, String warningHeader) {
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Transform [" + transformId + "]: " + HeaderWarning.extractWarningValueFromWarningHeader(warningHeader, true),
            TransformDeprecations.PAINLESS_BREAKING_CHANGES_URL,
            null,
            false,
            Collections.singletonMap(TransformField.TRANSFORM_ID, transformId)
        );
    }
}
