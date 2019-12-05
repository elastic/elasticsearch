/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action.compat;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;

public class PreviewTransformActionDeprecated extends ActionType<PreviewTransformAction.Response> {

    public static final PreviewTransformActionDeprecated INSTANCE = new PreviewTransformActionDeprecated();
    public static final String NAME = "cluster:admin/data_frame/preview";

    private PreviewTransformActionDeprecated() {
        super(NAME, PreviewTransformAction.Response::new);
    }


}
