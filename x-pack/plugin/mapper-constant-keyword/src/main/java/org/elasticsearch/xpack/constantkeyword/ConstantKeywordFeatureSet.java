/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.ConstantKeywordFeatureSetUsage;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Map;

public class ConstantKeywordFeatureSet implements XPackFeatureSet {

    private final XPackLicenseState licenseState;

    @Inject
    public ConstantKeywordFeatureSet(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.CONSTANT_KEYWORD;
    }

    @Override
    public boolean available() {
        return licenseState.isConstantKeywordAllowed();
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<Usage> listener) {
        listener.onResponse(new ConstantKeywordFeatureSetUsage(available(), enabled()));
    }

}
