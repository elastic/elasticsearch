/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.ArrayList;
import java.util.List;

public class TransformDeprecationChecker implements DeprecationChecker {

    public static final String TRANSFORM_DEPRECATION_KEY = "transform_settings";

    @Override
    public boolean enabled(Settings settings) {
        // always enabled
        return true;
    }

    @Override
    public void check(Components components, ActionListener<CheckResult> deprecationIssueListener) {

        PageParams startPage = new PageParams(0, PageParams.DEFAULT_SIZE);
        List<DeprecationIssue> issues = new ArrayList<>();
        recursiveGetTransformsAndCollectDeprecations(
            components,
            issues,
            startPage,
            deprecationIssueListener.delegateFailureAndWrap((l, allIssues) -> l.onResponse(new CheckResult(getName(), allIssues)))
        );
    }

    @Override
    public String getName() {
        return TRANSFORM_DEPRECATION_KEY;
    }

    private void recursiveGetTransformsAndCollectDeprecations(
        Components components,
        List<DeprecationIssue> issues,
        PageParams page,
        ActionListener<List<DeprecationIssue>> listener
    ) {
        final GetTransformAction.Request request = new GetTransformAction.Request(Metadata.ALL);
        request.setPageParams(page);
        request.setAllowNoResources(true);

        components.client()
            .execute(GetTransformAction.INSTANCE, request, listener.delegateFailureAndWrap((delegate, getTransformResponse) -> {
                for (TransformConfig config : getTransformResponse.getTransformConfigurations()) {
                    issues.addAll(config.checkForDeprecations(components.xContentRegistry()));
                }
                if (getTransformResponse.getTransformConfigurationCount() >= (page.getFrom() + page.getSize())) {
                    PageParams nextPage = new PageParams(page.getFrom() + page.getSize(), PageParams.DEFAULT_SIZE);
                    recursiveGetTransformsAndCollectDeprecations(components, issues, nextPage, delegate);
                } else {
                    delegate.onResponse(issues);
                }

            }));
    }
}
