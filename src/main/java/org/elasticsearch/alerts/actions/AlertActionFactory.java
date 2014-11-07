/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;


import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public interface AlertActionFactory {

    AlertAction createAction(XContentParser parser) throws IOException;

}
