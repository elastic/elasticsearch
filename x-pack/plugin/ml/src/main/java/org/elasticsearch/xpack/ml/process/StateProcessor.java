/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import java.io.IOException;
import java.io.InputStream;

public interface StateProcessor {

    void process(InputStream in) throws IOException;
}
