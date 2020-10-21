/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.eql.session.Payload;

public interface Executable {

    void execute(ActionListener<Payload> resultsListener);
}
