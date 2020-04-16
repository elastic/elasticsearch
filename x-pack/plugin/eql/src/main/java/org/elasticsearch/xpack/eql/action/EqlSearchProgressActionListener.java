/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.core.eql.action.EqlSearchProgressListener;
import org.elasticsearch.xpack.core.eql.action.EqlSearchResponse;


/**
 * An {@link ActionListener} for search requests that allows to track progress of the {@link EqlSearchAction}.
 * See {@link EqlSearchProgressListener}.
 */
public abstract class EqlSearchProgressActionListener extends EqlSearchProgressListener implements ActionListener<EqlSearchResponse> {
}
